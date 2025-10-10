.PHONY: help test test-integration test-unit test-all test-race test-coverage docker-up docker-down docker-logs clean

help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: test-all ## Run all tests

test-unit: ## Run unit tests
	@echo "Running unit tests..."
	@go test -v -short ./...

test-integration: docker-up ## Run integration tests
	@echo "Running integration tests..."
	@sleep 5
	@go test -v -timeout 3m ./rabbitmq || (docker compose -f docker-compose.test.yml logs && exit 1)

test-all: docker-up ## Run all tests including integration tests
	@echo "Running all tests..."
	@sleep 5
	@go test -v -timeout 3m ./... || (docker compose -f docker-compose.test.yml logs && exit 1)

test-race: docker-up ## Run tests with race detector
	@echo "Running tests with race detector..."
	@sleep 5
	@go test -race -timeout 5m ./rabbitmq || (docker compose -f docker-compose.test.yml logs && exit 1)

test-coverage: docker-up ## Run tests with coverage
	@echo "Running tests with coverage..."
	@sleep 5
	@go test -coverprofile=coverage.out -timeout 3m ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

docker-up: ## Start Docker test environment
	@echo "Starting Docker test environment..."
	@docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for RabbitMQ to be ready..."
	@timeout 30 sh -c 'until docker compose -f docker-compose.test.yml exec -T rabbitmq rabbitmq-diagnostics ping > /dev/null 2>&1; do sleep 1; done' || echo "Warning: RabbitMQ may not be fully ready"

docker-down: ## Stop Docker test environment
	@echo "Stopping Docker test environment..."
	@docker compose -f docker-compose.test.yml down

docker-logs: ## Show Docker container logs
	@docker compose -f docker-compose.test.yml logs

clean: docker-down ## Clean up test artifacts
	@echo "Cleaning up..."
	@rm -f coverage.out coverage.html
	@docker compose -f docker-compose.test.yml down -v
	@echo "Cleanup complete"

.DEFAULT_GOAL := help
