package rabbitmq_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nf5lab/broker"
	"github.com/nf5lab/broker/rabbitmq"
)

const (
	testRabbitMQURL      = "amqp://guest:guest@localhost:5672/"
	testExchangeName     = "test-exchange"
	testTimeout          = 30 * time.Second
	testShortTimeout     = 5 * time.Second
	testMessageWaitTime  = 2 * time.Second
)

// skipIfNoRabbitMQ skips the test if RabbitMQ is not available
func skipIfNoRabbitMQ(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") == "true" {
		t.Skip("Skipping integration test")
	}

	// Try to connect to verify RabbitMQ is available
	brk, err := rabbitmq.NewBroker(testRabbitMQURL, "health-check")
	if err != nil {
		t.Skipf("RabbitMQ not available: %v. Start it with: docker-compose -f docker-compose.test.yml up -d", err)
	}
	brk.Close()
}

// TestBasicPublishSubscribe tests basic message publishing and subscribing
func TestBasicPublishSubscribe(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Create broker
	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	// Message tracking
	var receivedCount atomic.Int32
	var receivedMsg *broker.Message
	var mu sync.Mutex
	done := make(chan struct{})

	// Subscribe to topic
	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		count := receivedCount.Add(1)
		if count == 1 {
			mu.Lock()
			receivedMsg = &delivery.Message
			mu.Unlock()
			close(done)
		}
		return nil
	}

	topic := "test.basic"
	_, err = brk.Subscribe(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait a bit for subscription to be ready
	time.Sleep(500 * time.Millisecond)

	// Publish message
	msg := &broker.Message{
		Id:          "test-msg-1",
		Body:        []byte("Hello, RabbitMQ!"),
		ContentType: "text/plain",
	}
	msg.AddHeader("custom-header", "custom-value")

	err = brk.Publish(ctx, topic, msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Wait for message
	select {
	case <-done:
		// Success
	case <-time.After(testMessageWaitTime):
		t.Fatal("Timeout waiting for message")
	}

	// Verify received message
	mu.Lock()
	defer mu.Unlock()

	if receivedMsg == nil {
		t.Fatal("No message received")
	}

	if receivedMsg.Id != msg.Id {
		t.Errorf("Expected message ID %s, got %s", msg.Id, receivedMsg.Id)
	}

	if string(receivedMsg.Body) != string(msg.Body) {
		t.Errorf("Expected body %s, got %s", string(msg.Body), string(receivedMsg.Body))
	}

	if receivedMsg.ContentType != msg.ContentType {
		t.Errorf("Expected content type %s, got %s", msg.ContentType, receivedMsg.ContentType)
	}

	if val, ok := receivedMsg.GetHeader("custom-header"); !ok || val != "custom-value" {
		t.Errorf("Expected custom-header to be 'custom-value', got %v", val)
	}
}

// TestMultipleSubscribers tests multiple subscribers receiving messages
func TestMultipleSubscribers(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.multiple"
	
	// Create two subscriber groups
	var group1Count, group2Count atomic.Int32
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	handler1 := func(ctx context.Context, delivery *broker.Delivery) error {
		if group1Count.Add(1) == 1 {
			close(done1)
		}
		return nil
	}

	handler2 := func(ctx context.Context, delivery *broker.Delivery) error {
		if group2Count.Add(1) == 1 {
			close(done2)
		}
		return nil
	}

	// Subscribe with different groups
	_, err = brk.Subscribe(ctx, topic, handler1, broker.WithSubscribeGroup("group1"))
	if err != nil {
		t.Fatalf("Failed to subscribe group1: %v", err)
	}

	_, err = brk.Subscribe(ctx, topic, handler2, broker.WithSubscribeGroup("group2"))
	if err != nil {
		t.Fatalf("Failed to subscribe group2: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Publish one message
	msg := &broker.Message{
		Id:   "test-msg-multi",
		Body: []byte("Message for multiple groups"),
	}

	err = brk.Publish(ctx, topic, msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Both groups should receive the message
	select {
	case <-done1:
		// Group1 received
	case <-time.After(testMessageWaitTime):
		t.Fatal("Timeout waiting for group1")
	}

	select {
	case <-done2:
		// Group2 received
	case <-time.After(testMessageWaitTime):
		t.Fatal("Timeout waiting for group2")
	}

	if group1Count.Load() != 1 {
		t.Errorf("Expected group1 to receive 1 message, got %d", group1Count.Load())
	}

	if group2Count.Load() != 1 {
		t.Errorf("Expected group2 to receive 1 message, got %d", group2Count.Load())
	}
}

// TestMessagePriority tests message priority handling
func TestMessagePriority(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.priority"
	var receivedIds []string
	var mu sync.Mutex
	expectedCount := 3
	done := make(chan struct{})

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		mu.Lock()
		receivedIds = append(receivedIds, delivery.Message.Id)
		if len(receivedIds) >= expectedCount {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	_, err = brk.Subscribe(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Publish messages with different priorities (higher priority first)
	// Note: Priority only matters when messages are queued
	messages := []struct {
		id       string
		priority int
	}{
		{"low-priority", 1},
		{"medium-priority", 5},
		{"high-priority", 10},
	}

	for _, m := range messages {
		msg := &broker.Message{
			Id:   m.id,
			Body: []byte(fmt.Sprintf("Priority %d message", m.priority)),
		}
		err = brk.Publish(ctx, topic, msg, broker.WithPublishPriority(m.priority))
		if err != nil {
			t.Fatalf("Failed to publish message %s: %v", m.id, err)
		}
	}

	select {
	case <-done:
		// Success
	case <-time.After(testMessageWaitTime):
		t.Fatal("Timeout waiting for messages")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedIds) != expectedCount {
		t.Errorf("Expected %d messages, got %d", expectedCount, len(receivedIds))
	}
}

// TestDelayedMessage tests delayed message delivery
func TestDelayedMessage(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Use unique exchange to avoid interference from other tests
	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName+"-delay")
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.delay"
	var receiveTime time.Time
	var mu sync.Mutex
	done := make(chan struct{})

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		mu.Lock()
		if receiveTime.IsZero() {
			receiveTime = time.Now()
			close(done)
		}
		mu.Unlock()
		return nil
	}

	_, err = brk.Subscribe(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait longer for subscription to be fully ready
	time.Sleep(1 * time.Second)

	// Publish delayed message
	// Note: RabbitMQ broker normalizes delays to predefined values (1s, 5s, 10s, etc.)
	// A 2-second delay will be normalized to 5 seconds
	requestedDelay := 2 * time.Second
	expectedNormalizedDelay := 5 * time.Second // Normalized delay
	publishTime := time.Now()
	t.Logf("Publishing delayed message at %v", publishTime)

	msg := &broker.Message{
		Id:   "delayed-msg",
		Body: []byte("This message is delayed"),
	}

	err = brk.Publish(ctx, topic, msg, broker.WithPublishDelay(requestedDelay))
	if err != nil {
		t.Fatalf("Failed to publish delayed message: %v", err)
	}

	select {
	case <-done:
		// Success
	case <-time.After(expectedNormalizedDelay + 5*time.Second):
		t.Fatal("Timeout waiting for delayed message")
	}

	mu.Lock()
	actualDelay := receiveTime.Sub(publishTime)
	t.Logf("Message received at %v", receiveTime)
	mu.Unlock()

	t.Logf("Requested delay: %v, Normalized delay: %v, Actual delay: %v", requestedDelay, expectedNormalizedDelay, actualDelay)

	// Allow some variance (should be at least the normalized delay time)
	if actualDelay < expectedNormalizedDelay-time.Second {
		t.Errorf("Message received too early: expected delay >= %v, got %v", expectedNormalizedDelay, actualDelay)
	}

	if actualDelay > expectedNormalizedDelay+3*time.Second {
		t.Errorf("Message received too late: expected delay ~ %v, got %v", expectedNormalizedDelay, actualDelay)
	}
}

// TestRetryMechanism tests message retry on failure
func TestRetryMechanism(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.retry"
	var attemptCount atomic.Int32
	done := make(chan struct{})

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		count := attemptCount.Add(1)
		if count < 3 {
			// Fail first 2 attempts
			return fmt.Errorf("simulated error on attempt %d", count)
		}
		// Succeed on 3rd attempt
		close(done)
		return nil
	}

	// Subscribe with retry settings
	_, err = brk.Subscribe(ctx, topic, handler, 
		broker.WithSubscribeMaxAttempts(5),
		broker.WithSubscribeRetryBackoff(func(retryCount int) time.Duration {
			// Fast retry for testing
			return 500 * time.Millisecond
		}),
	)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	msg := &broker.Message{
		Id:   "retry-msg",
		Body: []byte("This message will be retried"),
	}

	err = brk.Publish(ctx, topic, msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message retry")
	}

	finalCount := attemptCount.Load()
	if finalCount != 3 {
		t.Errorf("Expected 3 attempts, got %d", finalCount)
	}
}

// TestNonRetryableError tests that non-retryable errors don't trigger retries
func TestNonRetryableError(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.nonretryable"
	var attemptCount atomic.Int32

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		attemptCount.Add(1)
		// Return non-retryable error immediately
		err := fmt.Errorf("this error should not retry")
		return broker.NewNonRetryableError(err)
	}

	_, err = brk.Subscribe(ctx, topic, handler,
		broker.WithSubscribeMaxAttempts(5),
		broker.WithSubscribeRetryBackoff(func(retryCount int) time.Duration {
			return 200 * time.Millisecond
		}),
	)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	msg := &broker.Message{
		Id:   "nonretry-msg",
		Body: []byte("This message won't be retried"),
	}

	err = brk.Publish(ctx, topic, msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Wait a bit to ensure no retries happen
	time.Sleep(2 * time.Second)

	finalCount := attemptCount.Load()
	if finalCount != 1 {
		t.Errorf("Expected only 1 attempt for non-retryable error, got %d", finalCount)
	}
}

// TestConcurrentSubscribers tests concurrent message processing
func TestConcurrentSubscribers(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.concurrent"
	messageCount := 10
	var receivedCount atomic.Int32
	var processingCount atomic.Int32
	var maxConcurrent atomic.Int32
	done := make(chan struct{})

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		current := processingCount.Add(1)
		
		// Track max concurrent processing
		for {
			old := maxConcurrent.Load()
			if current <= old || maxConcurrent.CompareAndSwap(old, current) {
				break
			}
		}

		// Simulate some work
		time.Sleep(100 * time.Millisecond)
		
		processingCount.Add(-1)
		
		if receivedCount.Add(1) == int32(messageCount) {
			close(done)
		}
		return nil
	}

	// Subscribe with concurrency of 5
	concurrency := 5
	_, err = brk.Subscribe(ctx, topic, handler, 
		broker.WithSubscribeConcurrency(concurrency),
	)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Publish multiple messages
	for i := 0; i < messageCount; i++ {
		msg := &broker.Message{
			Id:   fmt.Sprintf("concurrent-msg-%d", i),
			Body: []byte(fmt.Sprintf("Message %d", i)),
		}
		err = brk.Publish(ctx, topic, msg)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	select {
	case <-done:
		// Success
	case <-time.After(testMessageWaitTime + 5*time.Second):
		t.Fatalf("Timeout waiting for messages. Received %d/%d", receivedCount.Load(), messageCount)
	}

	if receivedCount.Load() != int32(messageCount) {
		t.Errorf("Expected to receive %d messages, got %d", messageCount, receivedCount.Load())
	}

	// Verify concurrent processing happened
	maxConc := maxConcurrent.Load()
	if maxConc < 2 {
		t.Errorf("Expected concurrent processing (max >= 2), got max concurrent = %d", maxConc)
	}

	t.Logf("Max concurrent processing: %d (configured: %d)", maxConc, concurrency)
}

// TestUnsubscribe tests unsubscribing from a topic
func TestUnsubscribe(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.unsubscribe"
	var receivedCount atomic.Int32

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		receivedCount.Add(1)
		return nil
	}

	subId, err := brk.Subscribe(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Publish first message
	msg1 := &broker.Message{
		Id:   "unsub-msg-1",
		Body: []byte("Message before unsubscribe"),
	}
	err = brk.Publish(ctx, topic, msg1)
	if err != nil {
		t.Fatalf("Failed to publish first message: %v", err)
	}

	time.Sleep(testMessageWaitTime)

	// Unsubscribe
	err = brk.Unsubscribe(ctx, subId)
	if err != nil {
		t.Fatalf("Failed to unsubscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	countBeforeSecond := receivedCount.Load()

	// Publish second message after unsubscribe
	msg2 := &broker.Message{
		Id:   "unsub-msg-2",
		Body: []byte("Message after unsubscribe"),
	}
	err = brk.Publish(ctx, topic, msg2)
	if err != nil {
		t.Fatalf("Failed to publish second message: %v", err)
	}

	time.Sleep(testMessageWaitTime)

	countAfterSecond := receivedCount.Load()

	if countBeforeSecond != 1 {
		t.Errorf("Expected 1 message before unsubscribe, got %d", countBeforeSecond)
	}

	if countAfterSecond != countBeforeSecond {
		t.Errorf("Received message after unsubscribe: before=%d, after=%d", countBeforeSecond, countAfterSecond)
	}
}

// TestBrokerClose tests proper cleanup when closing the broker
func TestBrokerClose(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx := context.Background()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	topic := "test.close"
	var receivedCount atomic.Int32

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		receivedCount.Add(1)
		return nil
	}

	_, err = brk.Subscribe(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Close the broker
	err = brk.Close()
	if err != nil {
		t.Fatalf("Failed to close broker: %v", err)
	}

	// Try to publish after close (should fail)
	msg := &broker.Message{
		Id:   "after-close",
		Body: []byte("This should not be delivered"),
	}

	err = brk.Publish(ctx, topic, msg)
	if err == nil {
		t.Error("Expected error when publishing after close, got nil")
	}

	// Verify close is idempotent
	err = brk.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

// TestMessageTTL tests message time-to-live
func TestMessageTTL(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	topic := "test.ttl"
	var receivedCount atomic.Int32

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		receivedCount.Add(1)
		return nil
	}

	_, err = brk.Subscribe(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Publish message with short TTL
	msg := &broker.Message{
		Id:   "ttl-msg",
		Body: []byte("This message will expire"),
	}

	// Note: Testing TTL is tricky in integration tests as we'd need to
	// stop consuming for the TTL period. This test just verifies the API works.
	err = brk.Publish(ctx, topic, msg, broker.WithPublishTtl(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to publish message with TTL: %v", err)
	}

	time.Sleep(testMessageWaitTime)

	if receivedCount.Load() != 1 {
		t.Errorf("Expected to receive message before TTL expires, got %d", receivedCount.Load())
	}
}

// TestWildcardTopics tests topic pattern matching (if supported)
func TestTopicPatterns(t *testing.T) {
	skipIfNoRabbitMQ(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	brk, err := rabbitmq.NewBroker(testRabbitMQURL, testExchangeName)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer brk.Close()

	// Subscribe to wildcard topic
	pattern := "test.pattern.*"
	var receivedTopics []string
	var mu sync.Mutex
	expectedCount := 2
	done := make(chan struct{})

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		mu.Lock()
		receivedTopics = append(receivedTopics, delivery.Topic)
		if len(receivedTopics) >= expectedCount {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	_, err = brk.Subscribe(ctx, pattern, handler)
	if err != nil {
		t.Fatalf("Failed to subscribe to pattern: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Publish to matching topics
	topics := []string{"test.pattern.one", "test.pattern.two"}
	for i, topic := range topics {
		msg := &broker.Message{
			Id:   fmt.Sprintf("pattern-msg-%d", i),
			Body: []byte(fmt.Sprintf("Message for topic %s", topic)),
		}
		err = brk.Publish(ctx, topic, msg)
		if err != nil {
			t.Fatalf("Failed to publish to %s: %v", topic, err)
		}
	}

	select {
	case <-done:
		// Success
	case <-time.After(testMessageWaitTime):
		t.Fatalf("Timeout waiting for pattern messages. Received %d/%d", len(receivedTopics), expectedCount)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedTopics) != expectedCount {
		t.Errorf("Expected %d messages for pattern, got %d", expectedCount, len(receivedTopics))
	}

	// Verify both topics were received
	topicMap := make(map[string]bool)
	for _, topic := range receivedTopics {
		topicMap[topic] = true
	}

	for _, expected := range topics {
		if !topicMap[expected] {
			t.Errorf("Expected to receive message from topic %s", expected)
		}
	}
}
