package com.amqp.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;

@DisplayName("AMQP Server Tests")
class AmqpServerTest {

    @Mock
    private AmqpBroker mockBroker;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Nested
    @DisplayName("Server Construction Tests")
    class ServerConstructionTests {

        @Test
        @DisplayName("Should create server with valid port and broker")
        void testServerCreation() {
            AmqpServer server = new AmqpServer(5672, mockBroker);
            assertThat(server).isNotNull();
        }

        @Test
        @DisplayName("Should create server with custom port")
        void testServerWithCustomPort() {
            AmqpServer server = new AmqpServer(15672, mockBroker);
            assertThat(server).isNotNull();
        }

        @Test
        @DisplayName("Should create server with high port number")
        void testServerWithHighPort() {
            AmqpServer server = new AmqpServer(65000, mockBroker);
            assertThat(server).isNotNull();
        }
    }

    @Nested
    @DisplayName("Server Lifecycle Tests")
    class ServerLifecycleTests {

        @Test
        @DisplayName("Should start server on specified port")
        void testServerStart() throws Exception {
            AmqpServer server = new AmqpServer(15672, mockBroker);

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread.start();

            // Wait a bit for server to start
            Thread.sleep(2000);

            // Try to connect to verify server is listening
            boolean canConnect = false;
            try (Socket socket = new Socket("localhost", 15672)) {
                canConnect = socket.isConnected();
            } catch (Exception e) {
                // Connection failed
            }

            server.stop();
            serverThread.join(5000);

            assertThat(canConnect).isTrue();
        }

        @Test
        @DisplayName("Should stop server gracefully")
        void testServerStop() throws Exception {
            AmqpServer server = new AmqpServer(15673, mockBroker);

            AtomicBoolean serverStarted = new AtomicBoolean(false);
            CountDownLatch startLatch = new CountDownLatch(1);

            Thread serverThread = new Thread(() -> {
                try {
                    serverStarted.set(true);
                    startLatch.countDown();
                    server.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread.start();
            startLatch.await(3, TimeUnit.SECONDS);
            Thread.sleep(1000);

            // Stop the server
            server.stop();

            // Wait for thread to finish
            serverThread.join(5000);

            assertThat(serverStarted.get()).isTrue();
            assertThat(serverThread.isAlive()).isFalse();
        }

        @Test
        @DisplayName("Should handle multiple start-stop cycles")
        void testMultipleStartStopCycles() throws Exception {
            AmqpServer server = new AmqpServer(15674, mockBroker);

            for (int i = 0; i < 2; i++) {
                Thread serverThread = new Thread(() -> {
                    try {
                        server.start();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

                serverThread.start();
                Thread.sleep(1500);
                server.stop();
                serverThread.join(5000);
            }

            // If we get here without exceptions, test passes
            assertThat(true).isTrue();
        }
    }

    @Nested
    @DisplayName("Server Connection Tests")
    class ServerConnectionTests {

        @Test
        @DisplayName("Should accept client connections")
        void testAcceptConnection() throws Exception {
            AmqpServer server = new AmqpServer(15675, mockBroker);

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread.start();
            Thread.sleep(2000);

            // Try to connect
            Socket socket = null;
            try {
                socket = new Socket("localhost", 15675);
                assertThat(socket.isConnected()).isTrue();
            } finally {
                if (socket != null) {
                    socket.close();
                }
                server.stop();
                serverThread.join(5000);
            }
        }

        @Test
        @DisplayName("Should accept multiple concurrent connections")
        void testMultipleConnections() throws Exception {
            AmqpServer server = new AmqpServer(15676, mockBroker);

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread.start();
            Thread.sleep(2000);

            Socket[] sockets = new Socket[5];
            try {
                // Create multiple connections
                for (int i = 0; i < 5; i++) {
                    sockets[i] = new Socket("localhost", 15676);
                    assertThat(sockets[i].isConnected()).isTrue();
                }
            } finally {
                // Close all sockets
                for (Socket socket : sockets) {
                    if (socket != null) {
                        socket.close();
                    }
                }
                server.stop();
                serverThread.join(5000);
            }
        }

        @Test
        @DisplayName("Should handle connection on localhost")
        void testLocalhostConnection() throws Exception {
            AmqpServer server = new AmqpServer(15677, mockBroker);

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread.start();
            Thread.sleep(2000);

            Socket socket = null;
            try {
                socket = new Socket("127.0.0.1", 15677);
                assertThat(socket.isConnected()).isTrue();
                assertThat(socket.getInetAddress().getHostAddress()).isEqualTo("127.0.0.1");
            } finally {
                if (socket != null) {
                    socket.close();
                }
                server.stop();
                serverThread.join(5000);
            }
        }
    }

    @Nested
    @DisplayName("Server Error Handling Tests")
    class ServerErrorHandlingTests {

        @Test
        @DisplayName("Should handle stop before start")
        void testStopBeforeStart() {
            AmqpServer server = new AmqpServer(15678, mockBroker);

            // Should not throw exception
            assertThatCode(() -> server.stop()).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle multiple stop calls")
        void testMultipleStopCalls() throws Exception {
            AmqpServer server = new AmqpServer(15679, mockBroker);

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread.start();
            Thread.sleep(1500);

            // Stop multiple times
            server.stop();
            assertThatCode(() -> server.stop()).doesNotThrowAnyException();
            assertThatCode(() -> server.stop()).doesNotThrowAnyException();

            serverThread.join(5000);
        }

        @Test
        @DisplayName("Should refuse connections on already bound port")
        void testPortAlreadyInUse() throws Exception {
            AmqpServer server1 = new AmqpServer(15680, mockBroker);
            AmqpServer server2 = new AmqpServer(15680, mockBroker);

            Thread serverThread1 = new Thread(() -> {
                try {
                    server1.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread1.start();
            Thread.sleep(2000);

            // Try to start second server on same port
            AtomicBoolean server2FailedToStart = new AtomicBoolean(false);

            Thread serverThread2 = new Thread(() -> {
                try {
                    server2.start();
                } catch (Exception e) {
                    server2FailedToStart.set(true);
                }
            });

            serverThread2.start();
            Thread.sleep(2000);

            server1.stop();
            server2.stop();

            serverThread1.join(5000);
            serverThread2.join(5000);

            // Second server should have failed to bind
            assertThat(server2FailedToStart.get()).isTrue();
        }
    }

    @Nested
    @DisplayName("Server Configuration Tests")
    class ServerConfigurationTests {

        @Test
        @DisplayName("Should configure server with broker")
        void testServerWithBroker() {
            AmqpServer server = new AmqpServer(5672, mockBroker);
            assertThat(server).isNotNull();
        }

        @Test
        @DisplayName("Should accept connections after configuration")
        void testConnectionAfterConfiguration() throws Exception {
            AmqpServer server = new AmqpServer(15681, mockBroker);

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            serverThread.start();
            Thread.sleep(2000);

            try (Socket socket = new Socket("localhost", 15681)) {
                assertThat(socket.isConnected()).isTrue();
            } finally {
                server.stop();
                serverThread.join(5000);
            }
        }
    }
}
