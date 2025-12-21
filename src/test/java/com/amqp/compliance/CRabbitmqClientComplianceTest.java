package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using C rabbitmq-c client.
 *
 * Uses the official C AMQP client: rabbitmq-c
 * https://github.com/alanxz/rabbitmq-c
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("C rabbitmq-c Client Compliance Tests")
public class CRabbitmqClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(CRabbitmqClientComplianceTest.class);

    // C test using rabbitmq-c
    private static final String C_TEST_SOURCE = """
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>

void die_on_error(int x, char const *context) {
    if (x < 0) {
        fprintf(stderr, "FAIL: %s: %s\\n", context, amqp_error_string2(x));
        exit(1);
    }
}

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
    if (x.reply_type != AMQP_RESPONSE_NORMAL) {
        fprintf(stderr, "FAIL: %s\\n", context);
        exit(1);
    }
}

int main(int argc, char *argv[]) {
    char const *hostname = getenv("AMQP_HOST");
    if (!hostname) hostname = "amqp-server";

    char const *port_str = getenv("AMQP_PORT");
    int port = port_str ? atoi(port_str) : 5672;

    printf("Connecting to %s:%d\\n", hostname, port);

    amqp_connection_state_t conn;
    amqp_socket_t *socket = NULL;

    // Test 1: Connection
    conn = amqp_new_connection();
    socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        fprintf(stderr, "FAIL: Creating TCP socket\\n");
        return 1;
    }

    int status = amqp_socket_open(socket, hostname, port);
    die_on_error(status, "Opening TCP socket");

    die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
                      "Logging in");
    printf("PASS: Connection established\\n");

    // Test 2: Channel
    amqp_channel_open(conn, 1);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
    printf("PASS: Channel created\\n");

    // Test 3: Queue Declaration
    amqp_queue_declare_ok_t *r = amqp_queue_declare(
        conn, 1,
        amqp_cstring_bytes("c-test-queue"),
        0,  // passive
        0,  // durable
        0,  // exclusive
        1,  // auto_delete
        amqp_empty_table
    );
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
    amqp_bytes_t queue_name = amqp_bytes_malloc_dup(r->queue);
    printf("PASS: Queue declared: %.*s\\n", (int)queue_name.len, (char*)queue_name.bytes);

    // Test 4: Publish
    char const *message = "Hello from C rabbitmq-c!";
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");

    die_on_error(
        amqp_basic_publish(conn, 1,
                          amqp_cstring_bytes(""),
                          queue_name,
                          0, 0, &props,
                          amqp_cstring_bytes(message)),
        "Publishing"
    );
    printf("PASS: Message published\\n");

    // Test 5: Consume
    amqp_basic_consume(conn, 1, queue_name,
                      amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

    amqp_envelope_t envelope;
    struct timeval timeout = {5, 0};
    amqp_maybe_release_buffers(conn);

    amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);
    if (res.reply_type == AMQP_RESPONSE_NORMAL) {
        printf("PASS: Message received: %.*s\\n",
               (int)envelope.message.body.len, (char*)envelope.message.body.bytes);
        amqp_destroy_envelope(&envelope);
    } else {
        fprintf(stderr, "FAIL: Timeout or error receiving message\\n");
        return 1;
    }

    // Test 6: Exchange Declaration
    amqp_exchange_declare(conn, 1,
                         amqp_cstring_bytes("c-test-exchange"),
                         amqp_cstring_bytes("direct"),
                         0, 0, 1, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
    printf("PASS: Direct exchange declared\\n");

    // Test 7: Queue Bind
    amqp_queue_bind(conn, 1, queue_name,
                   amqp_cstring_bytes("c-test-exchange"),
                   amqp_cstring_bytes("test-key"),
                   amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");
    printf("PASS: Queue bound to exchange\\n");

    // Test 8: QoS
    amqp_basic_qos(conn, 1, 0, 10, 0);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Setting QoS");
    printf("PASS: QoS set\\n");

    // Test 9: Topic exchange
    amqp_exchange_declare(conn, 1,
                         amqp_cstring_bytes("c-topic-exchange"),
                         amqp_cstring_bytes("topic"),
                         0, 0, 1, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring topic exchange");
    printf("PASS: Topic exchange declared\\n");

    // Test 10: Fanout exchange
    amqp_exchange_declare(conn, 1,
                         amqp_cstring_bytes("c-fanout-exchange"),
                         amqp_cstring_bytes("fanout"),
                         0, 0, 1, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring fanout exchange");
    printf("PASS: Fanout exchange declared\\n");

    // Cleanup
    amqp_queue_delete(conn, 1, queue_name, 0, 0);
    amqp_exchange_delete(conn, 1, amqp_cstring_bytes("c-test-exchange"), 0);
    amqp_exchange_delete(conn, 1, amqp_cstring_bytes("c-topic-exchange"), 0);
    amqp_exchange_delete(conn, 1, amqp_cstring_bytes("c-fanout-exchange"), 0);

    amqp_bytes_free(queue_name);

    die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
    amqp_destroy_connection(conn);

    printf("\\n=== All C rabbitmq-c tests passed! ===\\n");
    return 0;
}
""";

    @Test
    @Order(1)
    @DisplayName("C: Run rabbitmq-c compliance tests")
    void testCRabbitmqClient() throws Exception {
        logger.info("Running C rabbitmq-c client tests...");

        GenericContainer<?> cClient = new GenericContainer<>("gcc:13")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withCommand("sh", "-c",
                        "apt-get update -qq && apt-get install -qq -y librabbitmq-dev > /dev/null 2>&1 && " +
                        "cat > test.c << 'CEOF'\n" + C_TEST_SOURCE + "\nCEOF\n" +
                        "gcc -o test test.c -lrabbitmq && " +
                        "./test")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("C-CLIENT"));

        String logs = runClientAndGetLogs(cClient);
        logger.info("C client output:\n{}", logs);

        assertTrue(logs.contains("All C rabbitmq-c tests passed!"),
                "C rabbitmq-c tests should pass. Output: " + logs);
    }
}
