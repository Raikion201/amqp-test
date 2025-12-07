package com.amqp.queue;

import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.model.QueueArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quorum Queue implementation using Raft consensus algorithm for high availability.
 * Quorum queues provide data safety by replicating messages across multiple nodes.
 *
 * Features:
 * - Leader election using Raft
 * - Log replication for messages
 * - Automatic failover on leader failure
 * - Configurable quorum size (typically 3 or 5 nodes)
 */
public class QuorumQueue extends Queue {
    private static final Logger logger = LoggerFactory.getLogger(QuorumQueue.class);

    private final QueueArguments arguments;
    private final int quorumSize;
    private final List<String> replicaNodes;

    // Raft state
    private volatile RaftState state;
    private volatile String leaderId;
    private final AtomicLong currentTerm;
    private volatile String votedFor;
    private final List<LogEntry> log;
    private final AtomicLong commitIndex;
    private final AtomicLong lastApplied;

    // For leader
    private final ConcurrentMap<String, Long> nextIndex;
    private final ConcurrentMap<String, Long> matchIndex;

    // Election timer
    private final ScheduledExecutorService electionScheduler;
    private volatile ScheduledFuture<?> electionTask;
    private static final long ELECTION_TIMEOUT_MIN = 150;
    private static final long ELECTION_TIMEOUT_MAX = 300;

    // Heartbeat timer
    private final ScheduledExecutorService heartbeatScheduler;
    private volatile ScheduledFuture<?> heartbeatTask;
    private static final long HEARTBEAT_INTERVAL = 50;

    private enum RaftState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    public QuorumQueue(String name, boolean durable, boolean exclusive,
                      boolean autoDelete, QueueArguments arguments, List<String> replicaNodes) {
        super(name, durable, exclusive, autoDelete);
        this.arguments = arguments != null ? arguments : new QueueArguments();
        this.replicaNodes = new ArrayList<>(replicaNodes);
        this.quorumSize = calculateQuorumSize(replicaNodes.size());

        // Initialize Raft state
        this.state = RaftState.FOLLOWER;
        this.currentTerm = new AtomicLong(0);
        this.log = new CopyOnWriteArrayList<>();
        this.commitIndex = new AtomicLong(0);
        this.lastApplied = new AtomicLong(0);
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();

        // Initialize schedulers
        this.electionScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "quorum-election-" + name));
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "quorum-heartbeat-" + name));

        startElectionTimer();
        logger.info("Quorum queue created: {} with {} replicas", name, replicaNodes.size());
    }

    private int calculateQuorumSize(int totalNodes) {
        return (totalNodes / 2) + 1;
    }

    @Override
    public void enqueue(Message message) {
        if (state != RaftState.LEADER) {
            throw new IllegalStateException("Not the leader - cannot enqueue to quorum queue: " + getName());
        }

        // Create log entry for the message
        LogEntry entry = new LogEntry(
            currentTerm.get(),
            log.size(),
            LogEntryType.MESSAGE,
            message
        );

        // Append to local log
        log.add(entry);
        logger.debug("Appended message to log at index {} on queue {}", entry.index, getName());

        // Replicate to followers (simplified - in real implementation would use Raft AppendEntries RPC)
        replicateToFollowers(entry);
    }

    @Override
    public Message dequeue() {
        if (state != RaftState.LEADER) {
            throw new IllegalStateException("Not the leader - cannot dequeue from quorum queue: " + getName());
        }

        // Only dequeue committed messages
        if (lastApplied.get() >= commitIndex.get()) {
            return null; // No committed messages available
        }

        long nextIdx = lastApplied.incrementAndGet();
        if (nextIdx < log.size()) {
            LogEntry entry = log.get((int) nextIdx);
            if (entry.type == LogEntryType.MESSAGE) {
                return entry.message;
            }
        }

        return null;
    }

    @Override
    public int size() {
        // Return number of committed messages
        return (int) (commitIndex.get() - lastApplied.get());
    }

    @Override
    public void purge() {
        if (state != RaftState.LEADER) {
            throw new IllegalStateException("Not the leader - cannot purge quorum queue: " + getName());
        }

        // Create purge log entry
        LogEntry entry = new LogEntry(
            currentTerm.get(),
            log.size(),
            LogEntryType.PURGE,
            null
        );

        log.add(entry);
        replicateToFollowers(entry);
    }

    /**
     * Replicate log entry to followers (simplified implementation).
     * Real implementation would use proper Raft AppendEntries RPC.
     */
    private void replicateToFollowers(LogEntry entry) {
        // Count this node as one replica
        int replicated = 1;

        // In a real implementation, would send AppendEntries RPC to each follower
        // and wait for responses. For now, we simulate immediate replication.

        // Simulate that quorum has acknowledged
        if (replicated >= quorumSize) {
            commitIndex.set(entry.index);
            logger.debug("Committed log entry at index {} on queue {}", entry.index, getName());
        }
    }

    /**
     * Start election timer. When it expires, node becomes candidate.
     */
    private void startElectionTimer() {
        long timeout = ThreadLocalRandom.current().nextLong(
            ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);

        electionTask = electionScheduler.schedule(
            this::startElection,
            timeout,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Start leader election.
     */
    private synchronized void startElection() {
        state = RaftState.CANDIDATE;
        long newTerm = currentTerm.incrementAndGet();
        votedFor = getNodeId(); // Vote for self

        logger.info("Starting election for term {} on queue {}", newTerm, getName());

        // Request votes from other nodes (simplified)
        // In real implementation, would send RequestVote RPC to all peers

        // For now, assume we win the election (single node scenario)
        becomeLeader();
    }

    /**
     * Become the leader.
     */
    private synchronized void becomeLeader() {
        state = RaftState.LEADER;
        leaderId = getNodeId();

        logger.info("Became leader for term {} on queue {}", currentTerm.get(), getName());

        // Cancel election timer
        if (electionTask != null) {
            electionTask.cancel(false);
        }

        // Initialize leader state
        for (String node : replicaNodes) {
            nextIndex.put(node, (long) log.size());
            matchIndex.put(node, 0L);
        }

        // Start sending heartbeats
        startHeartbeatTimer();
    }

    /**
     * Start heartbeat timer (leader only).
     */
    private void startHeartbeatTimer() {
        heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(
            this::sendHeartbeats,
            0,
            HEARTBEAT_INTERVAL,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Send heartbeats to all followers (AppendEntries with no entries).
     */
    private void sendHeartbeats() {
        if (state != RaftState.LEADER) {
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
            }
            return;
        }

        // In real implementation, would send AppendEntries RPC to all followers
        logger.trace("Sending heartbeats from leader on queue {}", getName());
    }

    /**
     * Get the current node ID (simplified - in real cluster would be actual node identifier).
     */
    private String getNodeId() {
        return "node-" + getName() + "-" + System.currentTimeMillis();
    }

    /**
     * Check if this node is the leader.
     */
    public boolean isLeader() {
        return state == RaftState.LEADER;
    }

    /**
     * Get the leader ID.
     */
    public String getLeaderId() {
        return leaderId;
    }

    /**
     * Get current Raft state.
     */
    public String getRaftState() {
        return state.name();
    }

    /**
     * Get current term.
     */
    public long getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Get replica nodes.
     */
    public List<String> getReplicaNodes() {
        return new ArrayList<>(replicaNodes);
    }

    /**
     * Shutdown the quorum queue.
     */
    public void shutdown() {
        logger.info("Shutting down quorum queue: {}", getName());

        if (electionTask != null) {
            electionTask.cancel(false);
        }
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }

        electionScheduler.shutdown();
        heartbeatScheduler.shutdown();

        try {
            electionScheduler.awaitTermination(5, TimeUnit.SECONDS);
            heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return String.format("QuorumQueue{name='%s', state=%s, term=%d, replicas=%d, size=%d}",
                           getName(), state, currentTerm.get(), replicaNodes.size(), size());
    }

    /**
     * Log entry for Raft.
     */
    private static class LogEntry {
        final long term;
        final long index;
        final LogEntryType type;
        final Message message;

        LogEntry(long term, long index, LogEntryType type, Message message) {
            this.term = term;
            this.index = index;
            this.type = type;
            this.message = message;
        }
    }

    private enum LogEntryType {
        MESSAGE,
        PURGE,
        CONFIGURATION
    }
}
