/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.impl;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class TransferLeadership {
  public static final Logger LOG = LoggerFactory.getLogger(TransferLeadership.class);

  private static class Context {
    private final TransferLeadershipRequest request;
    private final Supplier<TermIndex> leaderLastEntry;
    private final Supplier<LogAppender> transferee;

    Context(TransferLeadershipRequest request, Supplier<TermIndex> leaderLastEntry, Supplier<LogAppender> transferee) {
      this.request = request;
      this.leaderLastEntry = leaderLastEntry;
      this.transferee = transferee;
    }

    TransferLeadershipRequest getRequest() {
      return request;
    }

    RaftPeerId getTransfereeId() {
      return request.getNewLeader();
    }

    LogAppender getTransfereeLogAppender() {
      return transferee.get();
    }

    TermIndex getLeaderLastEntry() {
      return leaderLastEntry.get();
    }
  }

  class PendingRequest {
    private final TransferLeadershipRequest request;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();

    PendingRequest(TransferLeadershipRequest request) {
      this.request = request;
    }

    TransferLeadershipRequest getRequest() {
      return request;
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    void complete(String error) {
      if (replyFuture.isDone()) {
        return;
      }
      final RaftPeerId currentLeader = server.getState().getLeaderId();
      if (currentLeader != null && currentLeader.equals(request.getNewLeader())) {
        replyFuture.complete(server.newSuccessReply(request));
      } else {
        if (error == null) {
          error = "changed to a different leader";
        }
        final TransferLeadershipException tle = new TransferLeadershipException(server.getMemberId()
            + ": Failed to transfer leadership to " + request.getNewLeader()
            + " (the current leader is " + currentLeader + "): " + error);
        replyFuture.complete(server.newExceptionReply(request, tle));
      }
    }

    @Override
    public String toString() {
      return request.toString();
    }
  }

  private final RaftServerImpl server;
  private final TimeDuration requestTimeout;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  private final AtomicReference<PendingRequest> pending = new AtomicReference<>();

  TransferLeadership(RaftServerImpl server, RaftProperties properties) {
    this.server = server;
    this.requestTimeout = RaftServerConfigKeys.Rpc.requestTimeout(properties);
  }

  private Optional<RaftPeerId> getTransferee() {
    return Optional.ofNullable(pending.get())
        .map(r -> r.getRequest().getNewLeader());
  }

  boolean isSteppingDown() {
    return pending.get() != null;
  }

  void onFollowerAppendEntriesReply(LeaderStateImpl leaderState, FollowerInfo follower) {
    // If the transferee has just append some entries and becomes up-to-date,
    // send StartLeaderElection to it
    if (getTransferee().filter(t -> t.equals(follower.getId())).isPresent()) {
      final String error = leaderState.sendStartLeaderElection(follower, leaderState.getLastEntry());
      if (error == null) {
        LOG.info("{}: sent StartLeaderElection to transferee {} after received AppendEntriesResponse",
            server.getMemberId(), follower.getId());
      }
    }
  }

  private String tryTransferLeadership(LeaderStateImpl leaderState, Context context) {
    final RaftPeerId transferee = context.getTransfereeId();
    LOG.info("{}: start transferring leadership to {}", server.getMemberId(), transferee);
    final LogAppender appender = context.getTransfereeLogAppender();
    if (appender == null) {
      return "LogAppender for transferee " + transferee + " is null";
    }
    final FollowerInfo follower = appender.getFollower();
    final String error = leaderState.sendStartLeaderElection(follower, context.getLeaderLastEntry());
    if (error == null) {
      LOG.info("{}: sent StartLeaderElection to transferee {} immediately as it already has up-to-date log",
          server.getMemberId(), transferee);
    } else {
      appender.notifyLogAppender();
    }
    if (error != null && error.contains("not up-to-date")) {
      return null;
    }
    return error;
  }

  TransferLeadershipRequest newRequest(RaftPeerId transfereeId) {
    return new TransferLeadershipRequest(ClientId.emptyClientId(),
        server.getId(), server.getMemberId().getGroupId(), 0, transfereeId, 0);
  }

  void start(LeaderStateImpl leaderState, LogAppender transferee, TermIndex leaderLastEntry) {
    start(leaderState, new Context(newRequest(transferee.getFollowerId()), () -> leaderLastEntry, () -> transferee));
  }

  CompletableFuture<RaftClientReply> start(LeaderStateImpl leaderState, TransferLeadershipRequest request) {
    final Context context = new Context(request,
        JavaUtils.memoize(leaderState::getLastEntry),
        JavaUtils.memoize(() -> leaderState.getLogAppender(request.getNewLeader()).orElse(null)));
    return start(leaderState, context);
  }

  private CompletableFuture<RaftClientReply> start(LeaderStateImpl leaderState, Context context) {
    final TransferLeadershipRequest request = context.getRequest();
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(f -> f != null? f: supplier.get());
    if (previous != null) {
      return request != null? createReplyFutureFromPreviousRequest(request, previous): null;
    }
    final PendingRequest pendingRequest = supplier.get();
    final String error = tryTransferLeadership(leaderState, context);
    if (error != null) {
      pendingRequest.complete(error);
    } else {
      // if timeout is not specified in request, use default request timeout
      final TimeDuration timeout = request.getTimeoutMs() == 0 ? requestTimeout
          : TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS);
      scheduler.onTimeout(timeout, () -> complete("timed out " + timeout.toString(TimeUnit.SECONDS, 3)),
          LOG, () -> "Failed to transfer leadership to " + request.getNewLeader() + ": timeout after " + timeout);
    }
    return pendingRequest.getReplyFuture();
  }

  private CompletableFuture<RaftClientReply> createReplyFutureFromPreviousRequest(
      TransferLeadershipRequest request, PendingRequest previous) {
    if (request.getNewLeader().equals(previous.getRequest().getNewLeader())) {
      final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
      previous.getReplyFuture().whenComplete((r, e) -> {
        if (e != null) {
          replyFuture.completeExceptionally(e);
        } else {
          replyFuture.complete(r.isSuccess() ? server.newSuccessReply(request)
              : server.newExceptionReply(request, r.getException()));
        }
      });
      return replyFuture;
    } else {
      final TransferLeadershipException tle = new TransferLeadershipException(server.getMemberId() +
          "Failed to transfer leadership to " + request.getNewLeader() + ": a previous " + previous + " exists");
      return CompletableFuture.completedFuture(server.newExceptionReply(request, tle));
    }
  }

  void complete(String error) {
    Optional.ofNullable(pending.getAndSet(null))
        .ifPresent(r -> r.complete(error));
  }
}
