
### Prompt 1:
Create for me an AI Agent that will build for me an AMQP Server.

### Prompt 2:
## Role
You are a Senior Java Architect specializing in High-Performance Networking and Distributed Systems. You are an expert in **Netty 4.x** (NIO/Epoll), **PostgreSQL** schema design, and the **AMQP 0-9-1** protocol.

## Objective
Build a compliant AMQP Server using **Java** for the runtime, **Netty** for the asynchronous event-driven network layer, and **PostgreSQL** for persistence (durable exchanges, queues, and messages).

You must adhere to the **Iterative Development Protocol** below. Do not proceed to a new module until the current one passes all verification steps.

---

## The Iterative Development Protocol (Strict Workflow)

**You must execute the following loop for every feature:**

### Step 1: Define & Refine Requirements
**Action:** Define the technical specifications for the current module.
**Context:**
1.  **Netty Layer:** Define necessary ChannelHandlers (Decoders, Encoders, Business Logic).
2.  **AMQP Protocol:** Define specific frames (Method, Header, Body) and binary formats.
3.  **Persistence (PostgreSQL):** Define table schemas (DDL) and transactional requirements.

*Output:* A list of functional requirements, SQL schemas, and Netty pipeline design.

### Step 2: Implementation
**Action:** Write the Java code to satisfy Step 1.
**Constraints:**
- **Concurrency:** Use Netty's `EventLoopGroup` efficiently. Avoid blocking operations on I/O threads. Offload blocking DB calls to a separate `ExecutorService` or use a Reactive Driver.
- **Buffers:** Use `ByteBuf` for efficient memory management (zero-copy where possible).
- **Database:** Implement DAO/Repository patterns for PostgreSQL interactions.

*Output:* Java source code, Netty pipeline configuration, and SQL migration scripts.

### Step 3: Test Generation & Execution
**Action:** Write comprehensive tests for the code produced in Step 2.
**Mandate:**
- **Unit Tests:** Use JUnit 5 and Mockito.
- **Integration Tests:** Use **TestContainers** to spin up a real PostgreSQL instance and simulate a Netty client connecting to the server.
- **Protocol Tests:** Verify binary frame encoding/decoding matches AMQP spec.

*Output:* Test classes and simulated execution results.

### Step 4: Verification & Failure Loop
**Logic Check:**
1.  **Did any tests fail?**
    - **YES:** STOP. Analyze the stack trace. **Return to Step 2** to fix the implementation. If the design is flawed, **Return to Step 1**.
    - **NO:** Proceed to Logic Check 2.

2.  **Does the code strictly match the Requirements from Step 1?**
    - Perform a gap analysis between the written code and the requirements.
    - **GAP DETECTED:** **Return to Step 1** to include the missing scope or **Step 2** to implement it.
    - **MATCH:** Mark module as "DONE".

---

## Execution Trigger

**Current Task:** [Insert Module Name, e.g., "AMQP Frame Decoder" or "Postgres Queue Persistence"]

**Instruction:** Start at **Step 1** for the Current Task. Define the requirements (including Netty handlers and SQL schemas), then proceed to implementation.
Prompt

### Prompt 4:
Implement these fixes for the feedbacks, make sure to double check, then implement fixes if you see something wrong:

1. Security Vulnerabilities
Password Hashing is Weak (PasswordHasher.java:10-57)
- Uses SHA-256 which is NOT suitable for password hashing - it's too fast
- Should use bcrypt, scrypt, or Argon2 with proper work factors
- No iteration count/cost parameter



Guest User is Enabled by Default (AuthenticationManager.java:28-38)
- Default guest user with password guest has full administrator privileges
- This is a security nightmare in any production deployment



No Authentication on Management API (HttpManagementServer.java:116-300)
- HTTP management endpoints have zero authentication
- Anyone can create users, delete vhosts, see all data
- Line 286: ACCESS_CONTROL_ALLOW_ORIGIN: "*" - wide open CORS



No TLS by Default
- AMQP connections are unencrypted by default
- Credentials transmitted in plaintext



---
2. Incomplete/Broken Implementations



Headers Exchange Does Nothing (Exchange.java:224-226)
private List<String> routeHeaders(String routingKey) {
return new ArrayList<>(); // Always returns empty!
}



AMQP Channel Doesn't Actually Deliver Messages (AmqpChannel.java:315-323)
- handleBasicPublish() just logs and does nothing
- handleBasicConsume() registers consumer but never delivers messages
- No actual message flow between publish and consume



Transactions are Stub Implementations (AmqpChannel.java:402-421)
- txSelect, txCommit, txRollback just set a boolean flag
- No actual transaction isolation or rollback capability



Acknowledgements Don't Work (AmqpChannel.java:383-391)
public void acknowledgeMessage(long deliveryTag, boolean multiple) {
logger.debug("Acknowledging message...");
// Implementation would track unacknowledged messages and remove them
}
- Comments say "would" - it doesn't



---
3. Concurrency Bugs



Race Condition in Queue Name Generation (AmqpBroker.java:254-257)
private String generateQueueName() {
return "amq.gen-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
}
- Two requests at the same millisecond from the same thread = collision



Exchange Binding Race Condition (Exchange.java:70-78)
- removeBinding has check-then-act race: checking isEmpty() then remove() isn't atomic



FlowControl Memory Tracking Not Thread-Safe (FlowControl.java:51-58)
- addMemoryUsage and releaseMemoryUsage can interleave badly with threshold checks



---
4. Memory Leaks & Resource Issues



ByteBuf Leaks Everywhere (AmqpConnection.java:79-88, AmqpChannel.java)
- Creating Unpooled.buffer() without releasing them
- Netty reference-counted buffers need explicit release() or will leak



QuorumQueue Never Cleans Up Schedulers Properly (QuorumQueue.java:77-83)
- Creates ScheduledExecutorService in constructor
- shutdown() exists but nothing guarantees it's called
- Each queue creates 2 thread pools - no pooling



No Connection Limit
- Unbounded connections can exhaust server resources



---
5. Broken Persistence



Message ID Mismatch (PersistenceManager.java:95-131 vs AmqpBroker.java:198-218)
- saveMessage doesn't set the message ID from auto-generated DB column
- deleteMessage uses message.getId() which is likely 0
- Messages are persisted but never cleaned up properly



No Recovery on Startup
- PersistenceManager has loadMessages() but it's never called
- Durable exchanges/queues aren't restored from DB on restart



Bindings Aren't Persisted Per-VHost
- DB schema has no vhost column in bindings table
- Multi-vhost bindings will conflict



---
6. QuorumQueue is Fake Distributed (QuorumQueue.java)



Not Actually Distributed (lines 162-174)
private void replicateToFollowers(LogEntry entry) {
int replicated = 1; // Just counts itself
// "Simulate that quorum has acknowledged"
if (replicated >= quorumSize) { // Always true for single node
commitIndex.set(entry.index);
}
}
- Says "Raft consensus" but there's no network communication
- Replica nodes list is stored but never contacted
- Leader election always wins immediately (line 204)



---
7. API Design Problems



Deprecated Methods Everywhere (AmqpBroker.java)
- 6 deprecated methods that are still the primary code path
- Legacy methods bypass vhost/user checks, using hardcoded "guest"



Inconsistent Error Handling
- Some methods throw exceptions, others return null silently
- publishMessage just logs warning on missing exchange and returns (line 171-173)



No Input Validation
- Queue/Exchange names aren't validated
- Could create queues with null names, empty names, reserved prefixes



---
8. AMQP Protocol Issues



Incomplete Protocol Implementation (AmqpChannel.java)
- Missing: basic.get, basic.reject, basic.nack, basic.recover
- Missing: exchange.bind, exchange.unbind
- Missing: queue.unbind, queue.purge (method exists but not routed)
- Missing: Proper connection negotiation (SASL, heartbeat handling)



Frame Handling is Incomplete (AmqpChannel.java:345-355)
private void handleHeaderFrame(AmqpFrame frame) {
logger.debug("Received content header..."); // Does nothing
}
private void handleBodyFrame(AmqpFrame frame) {
logger.debug("Received content body..."); // Does nothing
}



---
9. Build/Dependency Issues



Spring Boot Plugin Without Spring (pom.xml:148-162)
- Uses spring-boot-maven-plugin but no Spring dependencies
- Odd choice for packaging



Testcontainers Version Mismatch Risk
- Multiple testcontainers artifacts could have version drift



---
10. Code Quality Issues



Magic Numbers Everywhere
- AmqpChannel.java: class ID 10, 20, 40, 50, 60 and method IDs without constants
- FlowControl.java: 400MB, 200MB, 50MB hardcoded
- Should use named constants from AMQP spec



Unused Variables (Exchange.java:140)
public List<String> route(String routingKey) {
Set<String> result = new HashSet<>(); // Never used!
switch (type) { ... }
}



Empty Deprecated Method (AmqpBroker.java:31-34)
@Deprecated
private void initializeDefaultExchanges() {
// This method is deprecated
}
Why does this exist?



---
Summary



This is essentially a skeleton/proof-of-concept that looks like an AMQP broker but:
1. Doesn't actually route messages end-to-end
2. Has serious security holes
3. Claims distributed features that are fake
4. Leaks resources
5. Can't recover from restarts
6. Would fail any real AMQP client compatibility test

### Prompt 5:
Update the readme.md

### Prompt 6:
Add gitignore for this project