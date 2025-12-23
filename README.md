# broker

A Go message broker abstraction library with RabbitMQ implementation.

## Features

- 🚀 **Simple API**: Easy-to-use publisher/subscriber interface
- 🔄 **Automatic Retry**: Built-in message retry with configurable backoff
- ⏱️ **Delayed Messages**: Support for delayed message delivery
- 🎯 **Priority Queues**: Message priority handling
- 🔌 **Concurrent Processing**: Configurable concurrency for subscribers
- 🏷️ **Topic Patterns**: Support for wildcard topic matching
- ⚡ **High Performance**: Connection pooling and efficient resource management
- 🧪 **Well Tested**: Comprehensive integration tests

## Installation

```bash
go get github.com/nf5lab/broker
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "github.com/nf5lab/broker"
    "github.com/nf5lab/broker/rabbitmq"
)

func main() {
    // Create broker
    brk, err := rabbitmq.NewBroker("amqp://guest:guest@localhost:5672/", "my-exchange")
    if err != nil {
        log.Fatal(err)
    }
    defer brk.Close()

    ctx := context.Background()

    // Subscribe to messages
    handler := func(ctx context.Context, delivery *broker.Delivery) error {
        log.Printf("Received: %s", string(delivery.Message.Body))
        return nil
    }

    _, err = brk.Subscribe(ctx, "my.topic", handler)
    if err != nil {
        log.Fatal(err)
    }

    // Publish a message
    msg := &broker.Message{
        Id:   "msg-1",
        Body: []byte("Hello, World!"),
    }

    err = brk.Publish(ctx, "my.topic", msg)
    if err != nil {
        log.Fatal(err)
    }

    // Keep running...
    select {}
}
```

## Testing

This project includes comprehensive integration tests for RabbitMQ.

### Prerequisites

- Docker and Docker Compose
- Go 1.25 or later

### Running Tests

Using Makefile (recommended):

```bash
# Run all tests (starts Docker automatically)
make test-all

# Run only integration tests
make test-integration

# Run tests with race detector
make test-race

# Generate coverage report
make test-coverage

# Clean up
make clean
```

Manual testing:

```bash
# Start RabbitMQ
docker compose -f docker-compose.test.yml up -d

# Run tests
go test -v ./rabbitmq

# Stop RabbitMQ
docker compose -f docker-compose.test.yml down
```

For more details, see [TESTING.md](TESTING.md).

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]