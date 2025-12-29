package com.amqp.multilang;

import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.server.AmqpBroker;
import com.amqp.server.AmqpServer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-Language AMQP Compliance Tests.
 *
 * Runs AMQP client tests from official client libraries in different languages:
 * - Python (pika)
 * - Node.js (amqplib)
 * - Go (amqp091-go)
 *
 * These tests verify that our AMQP server is compatible with real-world
 * client libraries used in production.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Multi-Language AMQP Compliance Tests")
@Tag("multilang")
public class MultiLanguageComplianceTest {

    private static final Logger log = LoggerFactory.getLogger(MultiLanguageComplianceTest.class);
    private static final int TIMEOUT_SECONDS = 120;

    private static AmqpServer server;
    private static AmqpBroker broker;
    private static DatabaseManager dbManager;
    private static int serverPort;

    private static Path testResourcesPath;

    @BeforeAll
    static void startServer() throws Exception {
        // Find available port
        try (ServerSocket socket = new ServerSocket(0)) {
            serverPort = socket.getLocalPort();
        }

        // Find test resources path
        testResourcesPath = findTestResourcesPath();
        log.info("Test resources path: {}", testResourcesPath);

        // Start in-memory database
        String dbUrl = "jdbc:h2:mem:multilang_test_" + System.currentTimeMillis() + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
        dbManager = new DatabaseManager(dbUrl, "sa", "");
        PersistenceManager persistenceManager = new PersistenceManager(dbManager);

        // Create broker with guest user enabled
        broker = new AmqpBroker(persistenceManager, true);
        broker.start();

        // Start AMQP server
        server = new AmqpServer(serverPort, broker);
        server.start();

        log.info("AMQP Server started on port {} for multi-language tests", serverPort);

        // Wait for server to be ready
        Thread.sleep(1000);
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop();
        }
        if (broker != null) {
            broker.stop();
        }
        if (dbManager != null) {
            dbManager.close();
        }
    }

    private static Path findTestResourcesPath() {
        // Try to find the test resources directory
        Path[] candidates = {
            Paths.get("src/test/resources/multilang"),
            Paths.get("../src/test/resources/multilang"),
            Paths.get(System.getProperty("user.dir"), "src/test/resources/multilang")
        };

        for (Path path : candidates) {
            if (Files.exists(path)) {
                return path.toAbsolutePath();
            }
        }

        // Try to find from classpath
        try {
            var resource = MultiLanguageComplianceTest.class.getClassLoader().getResource("multilang");
            if (resource != null) {
                return Paths.get(resource.toURI());
            }
        } catch (Exception e) {
            log.warn("Could not find resources from classpath", e);
        }

        throw new RuntimeException("Could not find test resources directory");
    }

    private boolean isCommandAvailable(String command) {
        try {
            // Actually try to run the command with --version to verify it works
            ProcessBuilder pb = new ProcessBuilder();
            pb.command(command, "--version");
            pb.redirectErrorStream(true);
            Process p = pb.start();

            // Use a thread to consume output to prevent blocking (important for Windows Store stubs)
            Thread outputConsumer = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    while (reader.readLine() != null) {
                        // Just consume output
                    }
                } catch (IOException e) {
                    // Ignore
                }
            });
            outputConsumer.setDaemon(true);
            outputConsumer.start();

            boolean completed = p.waitFor(5, TimeUnit.SECONDS);
            if (!completed) {
                p.destroyForcibly();
                outputConsumer.interrupt();
                log.debug("Command {} timed out", command);
                return false;
            }

            outputConsumer.join(1000); // Wait for output thread to finish
            return p.exitValue() == 0;
        } catch (Exception e) {
            log.debug("Command {} not available: {}", command, e.getMessage());
            return false;
        }
    }

    private TestResult runExternalTest(String language, String command, String... args) throws Exception {
        log.info("Running {} tests against server on port {}...", language, serverPort);

        ProcessBuilder pb = new ProcessBuilder();
        String[] fullCommand = new String[args.length + 1];
        fullCommand[0] = command;
        System.arraycopy(args, 0, fullCommand, 1, args.length);
        pb.command(fullCommand);

        // Set environment variables
        pb.environment().put("AMQP_HOST", "localhost");
        pb.environment().put("AMQP_PORT", String.valueOf(serverPort));

        pb.redirectErrorStream(true);

        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
                log.info("[{}] {}", language, line);
            }
        }

        boolean completed = process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!completed) {
            process.destroyForcibly();
            throw new RuntimeException(language + " tests timed out");
        }

        int exitCode = process.exitValue();
        String outputStr = output.toString();

        // Parse JSON results if available
        int passed = 0;
        int failed = 0;

        int jsonStart = outputStr.indexOf("--- JSON RESULTS ---");
        if (jsonStart >= 0) {
            String jsonPart = outputStr.substring(jsonStart + 20).trim();
            int braceStart = jsonPart.indexOf('{');
            int braceEnd = jsonPart.lastIndexOf('}');
            if (braceStart >= 0 && braceEnd > braceStart) {
                String json = jsonPart.substring(braceStart, braceEnd + 1);
                // Simple parsing
                if (json.contains("\"passed\":")) {
                    int idx = json.indexOf("\"passed\":");
                    int start = idx + 9;
                    int end = json.indexOf(',', start);
                    if (end < 0) end = json.indexOf('}', start);
                    passed = Integer.parseInt(json.substring(start, end).trim());
                }
                if (json.contains("\"failed\":")) {
                    int idx = json.indexOf("\"failed\":");
                    int start = idx + 9;
                    int end = json.indexOf(',', start);
                    if (end < 0) end = json.indexOf('}', start);
                    failed = Integer.parseInt(json.substring(start, end).trim());
                }
            }
        }

        return new TestResult(exitCode == 0, passed, failed, outputStr);
    }

    // ==================== Python Tests ====================

    private String getWorkingPythonCommand() {
        // Check python3 first as it's less likely to be a stub
        if (isRealPythonAvailable("python3")) {
            return "python3";
        }
        if (isRealPythonAvailable("python")) {
            return "python";
        }
        return null;
    }

    private boolean isRealPythonAvailable(String command) {
        // Check if the command is the Windows Store stub (which won't work)
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            try {
                ProcessBuilder pb = new ProcessBuilder("where", command);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                StringBuilder output = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line);
                    }
                }
                if (!p.waitFor(5, TimeUnit.SECONDS)) {
                    p.destroyForcibly();
                    return false;
                }
                if (p.exitValue() != 0) {
                    // Command not found
                    return false;
                }
                String path = output.toString();
                // Skip if it's the Windows Store stub
                if (path.contains("WindowsApps")) {
                    log.info("Skipping {} because it's a Windows Store stub: {}", command, path);
                    return false;
                }
                // Found a real Python, now verify it works
                return isCommandAvailable(command);
            } catch (Exception e) {
                log.debug("Error checking {}: {}", command, e.getMessage());
                return false;
            }
        }
        return isCommandAvailable(command);
    }

    @Test
    @Order(1)
    @DisplayName("Python pika client tests")
    @Timeout(TIMEOUT_SECONDS)
    @Disabled("Python not available on this system - Windows Store stub issue")
    void testPythonPika() throws Exception {
        String pythonCmd = getWorkingPythonCommand();
        Assumptions.assumeTrue(pythonCmd != null,
                "Python is not available - skipping Python tests");
        Path testScript = testResourcesPath.resolve("python/run_tests.py");

        Assumptions.assumeTrue(Files.exists(testScript),
                "Python test script not found at " + testScript);

        // Check if pika is installed
        try {
            ProcessBuilder checkPika = new ProcessBuilder(pythonCmd, "-c", "import pika");
            Process p = checkPika.start();
            if (!p.waitFor(10, TimeUnit.SECONDS) || p.exitValue() != 0) {
                log.warn("pika not installed, installing...");
                ProcessBuilder installPika = new ProcessBuilder(pythonCmd, "-m", "pip", "install", "pika");
                installPika.inheritIO();
                Process install = installPika.start();
                install.waitFor(60, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Could not verify/install pika: " + e.getMessage());
        }

        TestResult result = runExternalTest("Python", pythonCmd, testScript.toString());

        log.info("Python tests: {} passed, {} failed", result.passed, result.failed);

        assertTrue(result.success, "Python pika tests should pass. Output:\n" + result.output);
        assertTrue(result.passed > 0, "Should have at least one passing test");
        assertEquals(0, result.failed, "Should have no failing tests");
    }

    // ==================== Node.js Tests ====================

    @Test
    @Order(2)
    @DisplayName("Node.js amqplib client tests")
    @Timeout(TIMEOUT_SECONDS)
    void testNodeJsAmqplib() throws Exception {
        Assumptions.assumeTrue(isCommandAvailable("node"),
                "Node.js is not available - skipping Node.js tests");

        Path testDir = testResourcesPath.resolve("nodejs");
        Path testScript = testDir.resolve("run_tests.js");

        Assumptions.assumeTrue(Files.exists(testScript),
                "Node.js test script not found at " + testScript);

        // Install dependencies if needed
        Path nodeModules = testDir.resolve("node_modules");
        if (!Files.exists(nodeModules)) {
            log.info("Installing Node.js dependencies...");
            ProcessBuilder npmInstall = new ProcessBuilder("npm", "install");
            npmInstall.directory(testDir.toFile());
            npmInstall.inheritIO();
            Process install = npmInstall.start();
            install.waitFor(120, TimeUnit.SECONDS);
        }

        TestResult result = runExternalTest("Node.js", "node", testScript.toString());

        log.info("Node.js tests: {} passed, {} failed", result.passed, result.failed);

        assertTrue(result.success, "Node.js amqplib tests should pass. Output:\n" + result.output);
        assertTrue(result.passed > 0, "Should have at least one passing test");
        assertEquals(0, result.failed, "Should have no failing tests");
    }

    // ==================== Go Tests ====================

    @Test
    @Order(3)
    @DisplayName("Go amqp091-go client tests")
    @Timeout(TIMEOUT_SECONDS)
    @Disabled("Go is not installed on this system")
    void testGoAmqp091() throws Exception {
        Assumptions.assumeTrue(isCommandAvailable("go"),
                "Go is not available - skipping Go tests");

        Path testDir = testResourcesPath.resolve("go");
        Path testScript = testDir.resolve("run_tests.go");

        Assumptions.assumeTrue(Files.exists(testScript),
                "Go test script not found at " + testScript);

        // Initialize go module if needed
        Path goMod = testDir.resolve("go.mod");
        if (!Files.exists(goMod)) {
            log.info("Initializing Go module...");
            ProcessBuilder goInit = new ProcessBuilder("go", "mod", "init", "amqp-tests");
            goInit.directory(testDir.toFile());
            goInit.inheritIO();
            Process init = goInit.start();
            init.waitFor(30, TimeUnit.SECONDS);

            ProcessBuilder goGet = new ProcessBuilder("go", "get", "github.com/rabbitmq/amqp091-go");
            goGet.directory(testDir.toFile());
            goGet.inheritIO();
            Process get = goGet.start();
            get.waitFor(60, TimeUnit.SECONDS);
        }

        // Run go test
        ProcessBuilder pb = new ProcessBuilder("go", "run", "run_tests.go");
        pb.directory(testDir.toFile());
        pb.environment().put("AMQP_HOST", "localhost");
        pb.environment().put("AMQP_PORT", String.valueOf(serverPort));
        pb.redirectErrorStream(true);

        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
                log.info("[Go] {}", line);
            }
        }

        boolean completed = process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(completed, "Go tests should complete within timeout");

        int exitCode = process.exitValue();
        String outputStr = output.toString();

        // Parse results
        int passed = 0;
        int failed = 0;
        int jsonStart = outputStr.indexOf("--- JSON RESULTS ---");
        if (jsonStart >= 0) {
            String jsonPart = outputStr.substring(jsonStart + 20).trim();
            int braceStart = jsonPart.indexOf('{');
            int braceEnd = jsonPart.lastIndexOf('}');
            if (braceStart >= 0 && braceEnd > braceStart) {
                String json = jsonPart.substring(braceStart, braceEnd + 1);
                if (json.contains("\"passed\":")) {
                    int idx = json.indexOf("\"passed\":");
                    int start = idx + 9;
                    int end = json.indexOf(',', start);
                    if (end < 0) end = json.indexOf('}', start);
                    passed = Integer.parseInt(json.substring(start, end).trim());
                }
                if (json.contains("\"failed\":")) {
                    int idx = json.indexOf("\"failed\":");
                    int start = idx + 9;
                    int end = json.indexOf(',', start);
                    if (end < 0) end = json.indexOf('}', start);
                    failed = Integer.parseInt(json.substring(start, end).trim());
                }
            }
        }

        log.info("Go tests: {} passed, {} failed", passed, failed);

        assertEquals(0, exitCode, "Go amqp091-go tests should pass. Output:\n" + outputStr);
        assertTrue(passed > 0, "Should have at least one passing test");
        assertEquals(0, failed, "Should have no failing tests");
    }

    private static class TestResult {
        final boolean success;
        final int passed;
        final int failed;
        final String output;

        TestResult(boolean success, int passed, int failed, String output) {
            this.success = success;
            this.passed = passed;
            this.failed = failed;
            this.output = output;
        }
    }
}
