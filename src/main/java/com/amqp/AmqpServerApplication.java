package com.amqp;

import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.protocol.v10.server.Amqp10Server;
import com.amqp.server.AmqpBroker;
import com.amqp.server.AmqpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(AmqpServerApplication.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5672;
    private static final int DEFAULT_AMQP10_PORT = 5671;
    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5432/amqp";
    private static final String DEFAULT_DB_USER = "amcp";
    private static final String DEFAULT_DB_PASSWORD = "amcp";

    public static void main(String[] args) {
        try {
            AmqpServerConfig config = parseArguments(args);

            logger.info("Starting AMQP Server...");
            logger.info("Configuration: host={}, port={}, amqp10Port={}, database={}",
                       config.host, config.port, config.amqp10Port, config.databaseUrl);

            DatabaseManager databaseManager = new DatabaseManager(
                config.databaseUrl, config.databaseUser, config.databasePassword);

            PersistenceManager persistenceManager = new PersistenceManager(databaseManager);

            // Check for guest user environment variable
            boolean enableGuestUser = "true".equalsIgnoreCase(System.getenv("AMQP_GUEST_USER"));
            if (enableGuestUser) {
                logger.info("Guest user is ENABLED via AMQP_GUEST_USER environment variable");
            }

            AmqpBroker broker = new AmqpBroker(persistenceManager, enableGuestUser);

            // Start AMQP 0-9-1 server (in background thread since start() blocks)
            AmqpServer server091 = null;
            Thread server091Thread = null;
            if (config.enable091) {
                if (config.sslEnabled) {
                    logger.info("AMQP 0-9-1 SSL/TLS enabled with cert: {}, key: {}", config.sslCertPath, config.sslKeyPath);
                    server091 = new AmqpServer(config.port, broker, true, config.sslCertPath, config.sslKeyPath);
                } else {
                    server091 = new AmqpServer(config.port, broker);
                }
                final AmqpServer finalServer = server091;
                server091Thread = new Thread(() -> {
                    try {
                        finalServer.start();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.info("AMQP 0-9-1 server thread interrupted");
                    }
                }, "amqp-091-server");
                server091Thread.start();
                // Wait briefly for server to bind
                Thread.sleep(500);
                logger.info("AMQP 0-9-1 server started on port {}", config.port);
            }

            // Start AMQP 1.0 server
            Amqp10Server server10 = null;
            if (config.enable10) {
                server10 = new Amqp10Server(broker, config.amqp10Port);
                server10.setRequireSasl(!config.amqp10NoSasl);
                server10.start();
                logger.info("AMQP 1.0 server started on port {} (SASL: {})",
                           config.amqp10Port, !config.amqp10NoSasl);
            }

            // Capture references for shutdown hook
            final AmqpServer finalServer091 = server091;
            final Amqp10Server finalServer10 = server10;
            final Thread finalServer091Thread = server091Thread;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down AMQP Server...");
                if (finalServer091 != null) {
                    finalServer091.stop();
                }
                if (finalServer10 != null) {
                    finalServer10.shutdown();
                }
                if (finalServer091Thread != null) {
                    finalServer091Thread.interrupt();
                }
                databaseManager.close();
            }));

            // Keep main thread alive
            if (server091Thread != null) {
                // Block on 0-9-1 server thread
                server091Thread.join();
            } else if (server10 != null) {
                // Block on 1.0 server
                server10.awaitShutdown();
            }

        } catch (Exception e) {
            logger.error("Failed to start AMQP Server", e);
            System.exit(1);
        }
    }

    private static AmqpServerConfig parseArguments(String[] args) {
        AmqpServerConfig config = new AmqpServerConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host":
                    if (i + 1 < args.length) {
                        config.host = args[++i];
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        config.port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--amqp10-port":
                    if (i + 1 < args.length) {
                        config.amqp10Port = Integer.parseInt(args[++i]);
                        config.enable10 = true;
                    }
                    break;
                case "--enable-amqp10":
                    config.enable10 = true;
                    break;
                case "--disable-amqp091":
                    config.enable091 = false;
                    break;
                case "--amqp10-no-sasl":
                    config.amqp10NoSasl = true;
                    break;
                case "--db-url":
                    if (i + 1 < args.length) {
                        config.databaseUrl = args[++i];
                    }
                    break;
                case "--db-user":
                    if (i + 1 < args.length) {
                        config.databaseUser = args[++i];
                    }
                    break;
                case "--db-password":
                    if (i + 1 < args.length) {
                        config.databasePassword = args[++i];
                    }
                    break;
                case "--ssl-cert":
                    if (i + 1 < args.length) {
                        config.sslCertPath = args[++i];
                        config.sslEnabled = true;
                    }
                    break;
                case "--ssl-key":
                    if (i + 1 < args.length) {
                        config.sslKeyPath = args[++i];
                        config.sslEnabled = true;
                    }
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    if (args[i].startsWith("--")) {
                        System.err.println("Unknown option: " + args[i]);
                        printUsage();
                        System.exit(1);
                    }
            }
        }

        return config;
    }

    private static void printUsage() {
        System.out.println("AMQP Server - Dual-Protocol AMQP Server (0-9-1 and 1.0)");
        System.out.println();
        System.out.println("Usage: java -jar amqp-server.jar [OPTIONS]");
        System.out.println();
        System.out.println("Protocol Options:");
        System.out.println("  --port PORT           AMQP 0-9-1 server port (default: 5672)");
        System.out.println("  --amqp10-port PORT    AMQP 1.0 server port (default: 5671, enables AMQP 1.0)");
        System.out.println("  --enable-amqp10       Enable AMQP 1.0 on default port 5671");
        System.out.println("  --disable-amqp091     Disable AMQP 0-9-1 server");
        System.out.println("  --amqp10-no-sasl      Disable SASL authentication for AMQP 1.0");
        System.out.println();
        System.out.println("Database Options:");
        System.out.println("  --db-url URL          PostgreSQL JDBC URL (default: jdbc:postgresql://localhost:5432/amqp)");
        System.out.println("  --db-user USER        Database username (default: amcp)");
        System.out.println("  --db-password PASS    Database password (default: amcp)");
        System.out.println();
        System.out.println("Security Options:");
        System.out.println("  --ssl-cert PATH       SSL certificate file path (enables SSL/TLS for 0-9-1)");
        System.out.println("  --ssl-key PATH        SSL private key file path (enables SSL/TLS for 0-9-1)");
        System.out.println();
        System.out.println("Other Options:");
        System.out.println("  --host HOST           Server host (default: localhost)");
        System.out.println("  --help                Show this help message");
        System.out.println();
        System.out.println("Environment Variables:");
        System.out.println("  AMQP_GUEST_USER=true  Enable guest user authentication");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Run both AMQP 0-9-1 (5672) and AMQP 1.0 (5671)");
        System.out.println("  java -jar amqp-server.jar --enable-amqp10");
        System.out.println();
        System.out.println("  # Run only AMQP 1.0 on custom port");
        System.out.println("  java -jar amqp-server.jar --disable-amqp091 --amqp10-port 5673");
        System.out.println();
        System.out.println("  # Run with SSL/TLS");
        System.out.println("  java -jar amqp-server.jar --ssl-cert /path/to/cert.pem --ssl-key /path/to/key.pem");
    }

    private static class AmqpServerConfig {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        int amqp10Port = DEFAULT_AMQP10_PORT;
        boolean enable091 = true;
        boolean enable10 = false;
        boolean amqp10NoSasl = false;
        String databaseUrl = DEFAULT_DB_URL;
        String databaseUser = DEFAULT_DB_USER;
        String databasePassword = DEFAULT_DB_PASSWORD;
        boolean sslEnabled = false;
        String sslCertPath = null;
        String sslKeyPath = null;
    }
}
