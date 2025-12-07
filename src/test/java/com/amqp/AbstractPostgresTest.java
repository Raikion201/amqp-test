package com.amqp;

import com.amqp.persistence.DatabaseManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for tests that require PostgreSQL database.
 * Uses Testcontainers to automatically spin up a PostgreSQL instance.
 */
@Testcontainers
public abstract class AbstractPostgresTest {

    @Container
    protected static final PostgreSQLContainer<?> postgresContainer =
        new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("amcp_test")
            .withUsername("test")
            .withPassword("test")
            .withReuse(true);

    protected static DatabaseManager databaseManager;

    @BeforeAll
    public static void setUpDatabase() {
        postgresContainer.start();

        String jdbcUrl = postgresContainer.getJdbcUrl();
        String username = postgresContainer.getUsername();
        String password = postgresContainer.getPassword();

        databaseManager = new DatabaseManager(jdbcUrl, username, password);
    }

    @AfterAll
    public static void tearDownDatabase() {
        if (databaseManager != null) {
            databaseManager.close();
        }
    }

    protected DatabaseManager getDatabaseManager() {
        return databaseManager;
    }

    protected String getJdbcUrl() {
        return postgresContainer.getJdbcUrl();
    }

    protected String getUsername() {
        return postgresContainer.getUsername();
    }

    protected String getPassword() {
        return postgresContainer.getPassword();
    }
}
