package com.amqp;

import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.server.AmqpBroker;
import com.amqp.server.AmqpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpServerApplication {
    private static final Logger logger = LoggerFactory.getLogger(AmqpServerApplication.class);
    
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5672;
    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5432/amqp";
    private static final String DEFAULT_DB_USER = "amcp";
    private static final String DEFAULT_DB_PASSWORD = "amcp";
    
    public static void main(String[] args) {
        try {
            AmqpServerConfig config = parseArguments(args);
            
            logger.info("Starting AMQP Server...");
            logger.info("Configuration: host={}, port={}, database={}", 
                       config.host, config.port, config.databaseUrl);
            
            DatabaseManager databaseManager = new DatabaseManager(
                config.databaseUrl, config.databaseUser, config.databasePassword);
            
            PersistenceManager persistenceManager = new PersistenceManager(databaseManager);
            
            AmqpBroker broker = new AmqpBroker(persistenceManager);

            AmqpServer server;
            if (config.sslEnabled) {
                logger.info("SSL/TLS enabled with cert: {}, key: {}", config.sslCertPath, config.sslKeyPath);
                server = new AmqpServer(config.port, broker, true, config.sslCertPath, config.sslKeyPath);
            } else {
                server = new AmqpServer(config.port, broker);
            }
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down AMQP Server...");
                server.stop();
                databaseManager.close();
            }));
            
            server.start();
            
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
        System.out.println("AMQP Server - AMQP Server with PostgreSQL Backend");
        System.out.println();
        System.out.println("Usage: java -jar amqp-server.jar [OPTIONS]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --host HOST           Server host (default: localhost)");
        System.out.println("  --port PORT           Server port (default: 5672)");
        System.out.println("  --db-url URL          PostgreSQL JDBC URL (default: jdbc:postgresql://localhost:5432/amqp)");
        System.out.println("  --db-user USER        Database username (default: amqp)");
        System.out.println("  --db-password PASS    Database password (default: amqp)");
        System.out.println("  --ssl-cert PATH       SSL certificate file path (enables SSL/TLS)");
        System.out.println("  --ssl-key PATH        SSL private key file path (enables SSL/TLS)");
        System.out.println("  --help                Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar amqp-server.jar --port 5673 --db-url jdbc:postgresql://db:5432/amqp");
        System.out.println("  java -jar amqp-server.jar --ssl-cert /path/to/cert.pem --ssl-key /path/to/key.pem");
    }
    
    private static class AmqpServerConfig {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        String databaseUrl = DEFAULT_DB_URL;
        String databaseUser = DEFAULT_DB_USER;
        String databasePassword = DEFAULT_DB_PASSWORD;
        boolean sslEnabled = false;
        String sslCertPath = null;
        String sslKeyPath = null;
    }
}