# RabbitMQ Integration Tests

This directory contains integration tests for the RabbitMQ broker implementation.

## Prerequisites

- Docker and Docker Compose
- Go 1.25 or later

## Running the Tests

### 1. Start RabbitMQ Test Environment

First, start the RabbitMQ container:

```bash
docker-compose -f docker-compose.test.yml up -d
```

Wait a few seconds for RabbitMQ to be ready. You can check the status with:

```bash
docker-compose -f docker-compose.test.yml ps
```

### 2. Run Integration Tests

Run all integration tests:

```bash
go test -v ./rabbitmq
```

Run specific test:

```bash
go test -v ./rabbitmq -run TestBasicPublishSubscribe
```

Run with race detector:

```bash
go test -v -race ./rabbitmq
```

### 3. Stop RabbitMQ Test Environment

After testing, stop and remove the containers:

```bash
docker-compose -f docker-compose.test.yml down
```

To also remove volumes:

```bash
docker-compose -f docker-compose.test.yml down -v
```

## Skipping Integration Tests

To skip integration tests (e.g., in CI without RabbitMQ):

```bash
SKIP_INTEGRATION_TESTS=true go test ./rabbitmq
```

## Test Coverage

The integration tests cover:

- ✅ Basic publish/subscribe operations
- ✅ Multiple subscriber groups
- ✅ Message priority handling
- ✅ Delayed message delivery
- ✅ Retry mechanism with backoff
- ✅ Non-retryable errors
- ✅ Concurrent message processing
- ✅ Unsubscribe functionality
- ✅ Broker cleanup and close
- ✅ Message TTL (time-to-live)
- ✅ Topic pattern matching (wildcards)

## Accessing RabbitMQ Management UI

When the test environment is running, you can access the RabbitMQ Management UI at:

```
http://localhost:15672
```

Default credentials:
- Username: `guest`
- Password: `guest`

## Troubleshooting

### Tests are being skipped

Make sure RabbitMQ is running:
```bash
docker-compose -f docker-compose.test.yml up -d
```

### Port conflicts

If ports 5672 or 15672 are already in use, modify `docker-compose.test.yml` to use different ports.

### Connection refused errors

Wait a bit longer for RabbitMQ to fully start, or check the container logs:
```bash
docker-compose -f docker-compose.test.yml logs rabbitmq
```
