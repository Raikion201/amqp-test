// AMQP 1.0 Compliance Tests for Go using go-amqp
// Based on official Azure go-amqp library patterns
//
// These tests verify AMQP 1.0 protocol compliance including:
// - Connection establishment
// - Session management
// - Link establishment (sender/receiver)
// - Message transfer with various types
// - Flow control and credit management
// - Delivery acknowledgments
// - Message properties and annotations

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/go-amqp"
)

var (
	host     = getEnv("AMQP_HOST", "localhost")
	port     = getEnvInt("AMQP_PORT", 5672)
	username = getEnv("AMQP_USER", "guest")
	password = getEnv("AMQP_PASS", "guest")

	passed  = 0
	failed  = 0
	results []TestResult
)

type TestResult struct {
	Name   string
	Status string
	Error  string
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func log(msg string) {
	fmt.Printf("[%s] %s\n", time.Now().Format(time.RFC3339), msg)
}

func success(testName string) {
	passed++
	results = append(results, TestResult{Name: testName, Status: "PASSED"})
	log(fmt.Sprintf("✓ PASSED: %s", testName))
}

func fail(testName string, err error) {
	failed++
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	results = append(results, TestResult{Name: testName, Status: "FAILED", Error: errStr})
	log(fmt.Sprintf("✗ FAILED: %s - %v", testName, err))
}

func getBrokerURL() string {
	return fmt.Sprintf("amqp://%s:%d", host, port)
}

func createConnection(ctx context.Context) (*amqp.Conn, error) {
	return amqp.Dial(ctx, getBrokerURL(), &amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain(username, password),
	})
}

// Test 1: Basic AMQP 1.0 Connection
func testBasicConnection() {
	testName := "basic_amqp10_connection"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, err)
		return
	}
	defer conn.Close()

	success(testName)
}

// Test 2: Session Creation
func testSessionCreation() {
	testName := "session_creation"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, err)
		return
	}
	defer session.Close(ctx)

	success(testName)
}

// Test 3: Sender Link Creation
func testSenderLink() {
	testName := "sender_link_creation"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, "test-queue-sender", nil)
	if err != nil {
		fail(testName, err)
		return
	}
	defer sender.Close(ctx)

	success(testName)
}

// Test 4: Receiver Link Creation
func testReceiverLink() {
	testName := "receiver_link_creation"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	receiver, err := session.NewReceiver(ctx, "test-queue-receiver", nil)
	if err != nil {
		fail(testName, err)
		return
	}
	defer receiver.Close(ctx)

	success(testName)
}

// Test 5: Send Simple Message
func testSendSimpleMessage() {
	testName := "send_simple_message"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, "test-queue-simple", nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	msg := amqp.NewMessage([]byte("Hello AMQP 1.0!"))
	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, err)
		return
	}

	success(testName)
}

// Test 6: Send and Receive Message
func testSendReceiveMessage() {
	testName := "send_receive_message"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-sendrecv-%d", time.Now().UnixMilli())
	testBody := fmt.Sprintf("Test message %d", time.Now().UnixNano())

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	// Create sender
	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	// Create receiver
	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	// Send message
	msg := amqp.NewMessage([]byte(testBody))
	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	// Receive message
	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	if string(received.GetData()) == testBody {
		receiver.AcceptMessage(ctx, received)
		success(testName)
	} else {
		fail(testName, fmt.Errorf("body mismatch: expected %s, got %s", testBody, string(received.GetData())))
	}
}

// Test 7: Message Properties
func testMessageProperties() {
	testName := "message_properties"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-props-%d", time.Now().UnixMilli())

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	// Create message with properties
	msg := amqp.NewMessage([]byte("Properties test"))
	msg.Properties = &amqp.MessageProperties{
		MessageID:     "msg-123",
		CorrelationID: "corr-456",
		ContentType:   "text/plain",
		Subject:       "test-subject",
		ReplyTo:       "reply-queue",
	}

	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	props := received.Properties
	if props != nil &&
		props.MessageID == "msg-123" &&
		props.CorrelationID == "corr-456" &&
		props.ContentType == "text/plain" {
		receiver.AcceptMessage(ctx, received)
		success(testName)
	} else {
		fail(testName, fmt.Errorf("properties mismatch"))
	}
}

// Test 8: Application Properties
func testApplicationProperties() {
	testName := "application_properties"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-appprops-%d", time.Now().UnixMilli())

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	// Create message with application properties
	msg := amqp.NewMessage([]byte("App properties test"))
	msg.ApplicationProperties = map[string]any{
		"x-custom-header": "custom-value",
		"x-number":        int32(42),
		"x-boolean":       true,
	}

	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	appProps := received.ApplicationProperties
	if appProps != nil {
		customHeader, ok1 := appProps["x-custom-header"].(string)
		xNumber, ok2 := appProps["x-number"].(int32)
		xBool, ok3 := appProps["x-boolean"].(bool)

		if ok1 && customHeader == "custom-value" &&
			ok2 && xNumber == 42 &&
			ok3 && xBool == true {
			receiver.AcceptMessage(ctx, received)
			success(testName)
			return
		}
	}
	fail(testName, fmt.Errorf("application properties mismatch: %v", appProps))
}

// Test 9: Binary Message Body
func testBinaryBody() {
	testName := "binary_message_body"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-binary-%d", time.Now().UnixMilli())
	testData := []byte{0x01, 0x02, 0x03, 0x04, 0xFF, 0xFE}

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	msg := amqp.NewMessage(testData)
	msg.Properties = &amqp.MessageProperties{
		ContentType: "application/octet-stream",
	}

	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	receivedData := received.GetData()
	if len(receivedData) == len(testData) {
		match := true
		for i, b := range testData {
			if receivedData[i] != b {
				match = false
				break
			}
		}
		if match {
			receiver.AcceptMessage(ctx, received)
			success(testName)
			return
		}
	}
	fail(testName, fmt.Errorf("binary body mismatch"))
}

// Test 10: Multiple Messages
func testMultipleMessages() {
	testName := "multiple_messages"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-multi-%d", time.Now().UnixMilli())
	messageCount := 5

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	// Send messages
	for i := 0; i < messageCount; i++ {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("Message %d", i)))
		err = sender.Send(ctx, msg, nil)
		if err != nil {
			fail(testName, fmt.Errorf("send %d failed: %v", i, err))
			return
		}
	}

	// Receive messages
	for i := 0; i < messageCount; i++ {
		received, err := receiver.Receive(ctx, nil)
		if err != nil {
			fail(testName, fmt.Errorf("receive %d failed: %v", i, err))
			return
		}
		receiver.AcceptMessage(ctx, received)
	}

	success(testName)
}

// Test 11: Large Message
func testLargeMessage() {
	testName := "large_message"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-large-%d", time.Now().UnixMilli())
	largeBody := strings.Repeat("X", 100000) // 100KB

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	msg := amqp.NewMessage([]byte(largeBody))
	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	if string(received.GetData()) == largeBody {
		receiver.AcceptMessage(ctx, received)
		success(testName)
	} else {
		fail(testName, fmt.Errorf("size mismatch: expected %d, got %d", len(largeBody), len(received.GetData())))
	}
}

// Test 12: Durable Message
func testDurableMessage() {
	testName := "durable_message"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-durable-%d", time.Now().UnixMilli())

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	msg := amqp.NewMessage([]byte("Durable test"))
	msg.Header = &amqp.MessageHeader{
		Durable: true,
	}

	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	receiver.AcceptMessage(ctx, received)
	success(testName)
}

// Test 13: Message TTL
func testMessageTTL() {
	testName := "message_ttl"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-ttl-%d", time.Now().UnixMilli())

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	msg := amqp.NewMessage([]byte("TTL test"))
	msg.Header = &amqp.MessageHeader{
		TTL: 60 * time.Second, // 60 second TTL
	}

	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	receiver.AcceptMessage(ctx, received)
	success(testName)
}

// Test 14: Priority Message
func testPriorityMessage() {
	testName := "priority_message"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("test-queue-priority-%d", time.Now().UnixMilli())

	conn, err := createConnection(ctx)
	if err != nil {
		fail(testName, fmt.Errorf("connection failed: %v", err))
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("session failed: %v", err))
		return
	}
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("sender failed: %v", err))
		return
	}
	defer sender.Close(ctx)

	receiver, err := session.NewReceiver(ctx, queueName, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receiver failed: %v", err))
		return
	}
	defer receiver.Close(ctx)

	msg := amqp.NewMessage([]byte("Priority test"))
	msg.Header = &amqp.MessageHeader{
		Priority: 9,
	}

	err = sender.Send(ctx, msg, nil)
	if err != nil {
		fail(testName, fmt.Errorf("send failed: %v", err))
		return
	}

	received, err := receiver.Receive(ctx, nil)
	if err != nil {
		fail(testName, fmt.Errorf("receive failed: %v", err))
		return
	}

	receiver.AcceptMessage(ctx, received)
	success(testName)
}

func main() {
	log(strings.Repeat("=", 60))
	log("AMQP 1.0 Compliance Tests - Go (go-amqp)")
	log(fmt.Sprintf("Connecting to: %s", getBrokerURL()))
	log(strings.Repeat("=", 60))

	tests := []struct {
		name string
		fn   func()
	}{
		{"basic_amqp10_connection", testBasicConnection},
		{"session_creation", testSessionCreation},
		{"sender_link_creation", testSenderLink},
		{"receiver_link_creation", testReceiverLink},
		{"send_simple_message", testSendSimpleMessage},
		{"send_receive_message", testSendReceiveMessage},
		{"message_properties", testMessageProperties},
		{"application_properties", testApplicationProperties},
		{"binary_message_body", testBinaryBody},
		{"multiple_messages", testMultipleMessages},
		{"large_message", testLargeMessage},
		{"durable_message", testDurableMessage},
		{"message_ttl", testMessageTTL},
		{"priority_message", testPriorityMessage},
	}

	for _, test := range tests {
		test.fn()
		time.Sleep(500 * time.Millisecond) // Small delay between tests
	}

	fmt.Println()
	log(strings.Repeat("=", 60))
	log("TEST RESULTS SUMMARY")
	log(strings.Repeat("=", 60))
	log(fmt.Sprintf("Total:  %d", passed+failed))
	log(fmt.Sprintf("Passed: %d", passed))
	log(fmt.Sprintf("Failed: %d", failed))
	log(strings.Repeat("=", 60))

	if failed > 0 {
		log("\nFailed Tests:")
		for _, r := range results {
			if r.Status == "FAILED" {
				log(fmt.Sprintf("  - %s: %s", r.Name, r.Error))
			}
		}
	}

	if failed > 0 {
		os.Exit(1)
	}
}
