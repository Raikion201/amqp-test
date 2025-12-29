// AMQP 0-9-1 Compliance Tests using Go amqp091-go.
// Based on official amqp091-go test patterns.
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	amqpHost = getEnv("AMQP_HOST", "host.docker.internal")
	amqpPort = getEnv("AMQP_PORT", "5672")
	amqpURL  = fmt.Sprintf("amqp://guest:guest@%s:%s/", amqpHost, amqpPort)
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func randomString() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func waitForServer(host, port string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", host+":"+port, 5*time.Second)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

type TestResult struct {
	Test   string `json:"test"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type TestRunner struct {
	Results []TestResult
	Passed  int
	Failed  int
}

func (t *TestRunner) runTest(name string, testFunc func() error) bool {
	err := testFunc()
	if err == nil {
		t.Results = append(t.Results, TestResult{Test: name, Status: "PASS"})
		t.Passed++
		fmt.Printf("  [PASS] %s\n", name)
		return true
	}
	t.Results = append(t.Results, TestResult{Test: name, Status: "FAIL", Error: err.Error()})
	t.Failed++
	fmt.Printf("  [FAIL] %s: %v\n", name, err)
	return false
}

// ==================== Connection Tests ====================

func testConnectionOpen() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("dial failed: %v", err)
	}
	defer conn.Close()
	return nil
}

func testChannelOpen() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("dial failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel failed: %v", err)
	}
	defer ch.Close()
	return nil
}

// ==================== Queue Tests ====================

func testQueueDeclare() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	queueName := "test.go.queue." + randomString()
	q, err := ch.QueueDeclare(queueName, false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue declare failed: %v", err)
	}
	if q.Name != queueName {
		return fmt.Errorf("queue name mismatch: %s != %s", q.Name, queueName)
	}

	_, err = ch.QueueDelete(queueName, false, false, false)
	return err
}

func testQueueDeclareExclusive() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return fmt.Errorf("exclusive queue declare failed: %v", err)
	}
	if q.Name == "" {
		return fmt.Errorf("exclusive queue should have name")
	}
	return nil
}

// ==================== Exchange Tests ====================

func testExchangeDeclareDirect() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	exName := "test.go.direct." + randomString()
	if err := ch.ExchangeDeclare(exName, "direct", false, true, false, false, nil); err != nil {
		return err
	}
	return ch.ExchangeDelete(exName, false, false)
}

func testExchangeDeclareFanout() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	exName := "test.go.fanout." + randomString()
	if err := ch.ExchangeDeclare(exName, "fanout", false, true, false, false, nil); err != nil {
		return err
	}
	return ch.ExchangeDelete(exName, false, false)
}

func testExchangeDeclareTopic() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	exName := "test.go.topic." + randomString()
	if err := ch.ExchangeDeclare(exName, "topic", false, true, false, false, nil); err != nil {
		return err
	}
	return ch.ExchangeDelete(exName, false, false)
}

// ==================== Binding Tests ====================

func testQueueBind() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	exName := "test.go.bind.ex." + randomString()
	qName := "test.go.bind.q." + randomString()

	if err := ch.ExchangeDeclare(exName, "direct", false, true, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(qName, false, true, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(qName, "test.key", exName, false, nil); err != nil {
		return err
	}
	if err := ch.QueueUnbind(qName, "test.key", exName, nil); err != nil {
		return err
	}

	ch.QueueDelete(qName, false, false, false)
	ch.ExchangeDelete(exName, false, false)
	return nil
}

// ==================== Publish/Consume Tests ====================

func testBasicPublishConsume() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	qName := "test.go.pubsub." + randomString()
	testMsg := "Hello from Go! " + randomString()

	if _, err := ch.QueueDeclare(qName, false, true, false, false, nil); err != nil {
		return err
	}

	if err := ch.Publish("", qName, false, false, amqp.Publishing{
		Body: []byte(testMsg),
	}); err != nil {
		return err
	}

	msgs, err := ch.Consume(qName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != testMsg {
			return fmt.Errorf("message mismatch: %s != %s", string(msg.Body), testMsg)
		}
		msg.Ack(false)
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for message")
	}

	ch.QueueDelete(qName, false, false, false)
	return nil
}

func testPublishWithProperties() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	qName := "test.go.props." + randomString()

	if _, err := ch.QueueDeclare(qName, false, true, false, false, nil); err != nil {
		return err
	}

	if err := ch.Publish("", qName, false, false, amqp.Publishing{
		Body:          []byte("test"),
		ContentType:   "text/plain",
		CorrelationId: "corr-123",
		MessageId:     "msg-456",
	}); err != nil {
		return err
	}

	msgs, err := ch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	select {
	case msg := <-msgs:
		if msg.ContentType != "text/plain" {
			return fmt.Errorf("contentType mismatch: %s", msg.ContentType)
		}
		if msg.CorrelationId != "corr-123" {
			return fmt.Errorf("correlationId mismatch: %s", msg.CorrelationId)
		}
		if msg.MessageId != "msg-456" {
			return fmt.Errorf("messageId mismatch: %s", msg.MessageId)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for message")
	}

	ch.QueueDelete(qName, false, false, false)
	return nil
}

func testPublishWithHeaders() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	qName := "test.go.headers." + randomString()

	if _, err := ch.QueueDeclare(qName, false, true, false, false, nil); err != nil {
		return err
	}

	headers := amqp.Table{
		"x-custom": "value",
		"x-number": int32(42),
	}

	if err := ch.Publish("", qName, false, false, amqp.Publishing{
		Body:    []byte("test"),
		Headers: headers,
	}); err != nil {
		return err
	}

	msgs, err := ch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	select {
	case msg := <-msgs:
		if msg.Headers == nil {
			return fmt.Errorf("headers should exist")
		}
		if msg.Headers["x-custom"] != "value" {
			return fmt.Errorf("header mismatch: %v", msg.Headers["x-custom"])
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for message")
	}

	ch.QueueDelete(qName, false, false, false)
	return nil
}

func testBasicNackRequeue() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	qName := "test.go.nack." + randomString()

	if _, err := ch.QueueDeclare(qName, false, true, false, false, nil); err != nil {
		return err
	}

	if err := ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte("nack-me")}); err != nil {
		return err
	}

	msgs, err := ch.Consume(qName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	deliveryCount := 0
	for deliveryCount < 2 {
		select {
		case msg := <-msgs:
			deliveryCount++
			if deliveryCount == 1 {
				msg.Nack(false, true)
			} else {
				msg.Ack(false)
			}
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout waiting for message")
		}
	}

	if deliveryCount != 2 {
		return fmt.Errorf("expected 2 deliveries, got %d", deliveryCount)
	}

	ch.QueueDelete(qName, false, false, false)
	return nil
}

func testBasicQos() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.Qos(10, 0, false)
}

func testQueuePurge() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	qName := "test.go.purge." + randomString()

	if _, err := ch.QueueDeclare(qName, false, true, false, false, nil); err != nil {
		return err
	}

	for i := 0; i < 5; i++ {
		ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte(fmt.Sprintf("msg-%d", i))})
	}

	time.Sleep(100 * time.Millisecond)

	count, err := ch.QueuePurge(qName, false)
	if err != nil {
		return err
	}
	if count != 5 {
		return fmt.Errorf("expected 5 purged, got %d", count)
	}

	ch.QueueDelete(qName, false, false, false)
	return nil
}

func testDirectRouting() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	exName := "test.go.routing.direct." + randomString()
	q1 := "test.go.routing.q1." + randomString()
	q2 := "test.go.routing.q2." + randomString()

	ch.ExchangeDeclare(exName, "direct", false, true, false, false, nil)
	ch.QueueDeclare(q1, false, true, false, false, nil)
	ch.QueueDeclare(q2, false, true, false, false, nil)
	ch.QueueBind(q1, "key1", exName, false, nil)
	ch.QueueBind(q2, "key2", exName, false, nil)

	ch.Publish(exName, "key1", false, false, amqp.Publishing{Body: []byte("to-q1")})
	ch.Publish(exName, "key2", false, false, amqp.Publishing{Body: []byte("to-q2")})

	time.Sleep(100 * time.Millisecond)

	msg1, _, _ := ch.Get(q1, true)
	msg2, _, _ := ch.Get(q2, true)

	if string(msg1.Body) != "to-q1" {
		return fmt.Errorf("routing to q1 failed: %s", string(msg1.Body))
	}
	if string(msg2.Body) != "to-q2" {
		return fmt.Errorf("routing to q2 failed: %s", string(msg2.Body))
	}

	ch.QueueDelete(q1, false, false, false)
	ch.QueueDelete(q2, false, false, false)
	ch.ExchangeDelete(exName, false, false)
	return nil
}

func testFanoutRouting() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	exName := "test.go.routing.fanout." + randomString()
	q1 := "test.go.fanout.q1." + randomString()
	q2 := "test.go.fanout.q2." + randomString()

	ch.ExchangeDeclare(exName, "fanout", false, true, false, false, nil)
	ch.QueueDeclare(q1, false, true, false, false, nil)
	ch.QueueDeclare(q2, false, true, false, false, nil)
	ch.QueueBind(q1, "", exName, false, nil)
	ch.QueueBind(q2, "", exName, false, nil)

	ch.Publish(exName, "", false, false, amqp.Publishing{Body: []byte("broadcast")})

	time.Sleep(100 * time.Millisecond)

	msg1, _, _ := ch.Get(q1, true)
	msg2, _, _ := ch.Get(q2, true)

	if string(msg1.Body) != "broadcast" {
		return fmt.Errorf("fanout to q1 failed")
	}
	if string(msg2.Body) != "broadcast" {
		return fmt.Errorf("fanout to q2 failed")
	}

	ch.QueueDelete(q1, false, false, false)
	ch.QueueDelete(q2, false, false, false)
	ch.ExchangeDelete(exName, false, false)
	return nil
}

func testMessageOrdering() error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	qName := "test.go.order." + randomString()

	if _, err := ch.QueueDeclare(qName, false, true, false, false, nil); err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		ch.Publish("", qName, false, false, amqp.Publishing{Body: []byte(fmt.Sprintf("msg-%d", i))})
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		msg, _, _ := ch.Get(qName, true)
		expected := fmt.Sprintf("msg-%d", i)
		if string(msg.Body) != expected {
			return fmt.Errorf("out of order at %d: %s != %s", i, string(msg.Body), expected)
		}
	}

	ch.QueueDelete(qName, false, false, false)
	return nil
}

func (t *TestRunner) runAll() map[string]interface{} {
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Go amqp091-go Official Test Suite")
	fmt.Printf("Server: %s:%s\n", amqpHost, amqpPort)
	fmt.Println("============================================================")
	fmt.Println()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"connection_open", testConnectionOpen},
		{"channel_open", testChannelOpen},
		{"queue_declare", testQueueDeclare},
		{"queue_declare_exclusive", testQueueDeclareExclusive},
		{"exchange_declare_direct", testExchangeDeclareDirect},
		{"exchange_declare_fanout", testExchangeDeclareFanout},
		{"exchange_declare_topic", testExchangeDeclareTopic},
		{"queue_bind", testQueueBind},
		{"basic_publish_consume", testBasicPublishConsume},
		{"publish_with_properties", testPublishWithProperties},
		{"publish_with_headers", testPublishWithHeaders},
		{"basic_nack_requeue", testBasicNackRequeue},
		{"basic_qos", testBasicQos},
		{"queue_purge", testQueuePurge},
		{"direct_routing", testDirectRouting},
		{"fanout_routing", testFanoutRouting},
		{"message_ordering", testMessageOrdering},
	}

	for _, test := range tests {
		t.runTest(test.name, test.fn)
	}

	fmt.Println()
	fmt.Println("============================================================")
	fmt.Printf("Results: %d passed, %d failed\n", t.Passed, t.Failed)
	fmt.Println("============================================================")
	fmt.Println()

	return map[string]interface{}{
		"passed":  t.Passed,
		"failed":  t.Failed,
		"results": t.Results,
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Printf("Waiting for AMQP server at %s:%s...\n", amqpHost, amqpPort)

	if !waitForServer(amqpHost, amqpPort, 60*time.Second) {
		fmt.Println("ERROR: AMQP server not available")
		os.Exit(1)
	}

	fmt.Println("Server is available, running tests...")

	runner := &TestRunner{}
	results := runner.runAll()

	fmt.Println("\n--- JSON RESULTS ---")
	jsonBytes, _ := json.Marshal(results)
	fmt.Println(string(jsonBytes))

	if runner.Failed > 0 {
		os.Exit(1)
	}
}
