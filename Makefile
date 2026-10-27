GO            ?= go
BIN           := $(CURDIR)/bin
COMPOSE       := docker compose -f deploy/compose/docker-compose.yml
SERVICES      := controlplane worker aggregator calibrate
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REGISTRY      ?= loadforge
GOFILES        = $(shell find . -name '*.go' -not -path './bin/*' -not -path './.cache/*')

GOLANGCI_LINT_VERSION := v1.61.0
GOTESTSUM_VERSION     := v1.12.0
GOLANGCI_LINT         := $(BIN)/golangci-lint
GOTESTSUM             := $(BIN)/gotestsum

KAFKA_TOPICS := /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092

.DEFAULT_GOAL := help
.PHONY: help test test-integration lint fmt build images up down tools

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-18s %s\n",$$1,$$2}'

test: $(GOTESTSUM) ## Unit tests with the race detector
	@mkdir -p reports
	$(GOTESTSUM) --junitfile reports/unit.xml -- -race -count=1 ./...

test-integration: $(GOTESTSUM) ## Integration tests (run `make up` first)
	@mkdir -p reports
	$(GOTESTSUM) --junitfile reports/integration.xml -- -race -count=1 -tags=integration ./...

lint: $(GOLANGCI_LINT) ## gofmt, go vet and golangci-lint
	@out=$$(gofmt -l $(GOFILES)); \
	  if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	$(GO) vet ./...
	$(GOLANGCI_LINT) run ./...

fmt: ## Format the code
	gofmt -w $(GOFILES)

build: ## Build every binary into ./bin
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN)/ ./cmd/...

images: ## Build one container image per service
	@for svc in $(SERVICES); do \
	  docker build --build-arg SERVICE=$$svc -t $(REGISTRY)/$$svc:$(VERSION) . || exit 1; \
	done

up: ## Start Postgres, Redis, NATS and Kafka, then create the topics
	$(COMPOSE) up -d --wait
	$(COMPOSE) exec -T kafka $(KAFKA_TOPICS) --create --if-not-exists \
	  --topic loadforge.hdr.snapshots --partitions 12 --replication-factor 1

down: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v --remove-orphans

tools: $(GOLANGCI_LINT) $(GOTESTSUM) ## Install the pinned dev tools into ./bin

$(GOLANGCI_LINT):
	GOBIN=$(BIN) $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(GOTESTSUM):
	GOBIN=$(BIN) $(GO) install gotest.tools/gotestsum@$(GOTESTSUM_VERSION)
