.DEFAULT_GOAL := all

GOCMD := go

MEASUREMENTS_BIN := create_measurements
MEASUREMENTS_DIR := cmd/measurements

ONEBRC_BIN := 1brc
ONEBRC_DIR := cmd/onebrc

.PHONY: all
all: build

.PHONY: build
build: $(MEASUREMENTS_BIN) $(ONEBRC_BIN)

.PHONY: $(MEASUREMENTS_BIN)
$(MEASUREMENTS_BIN):
	$(GOCMD) build -o $(MEASUREMENTS_BIN).exe ./$(MEASUREMENTS_DIR)

.PHONY: $(ONEBRC_BIN)
$(ONEBRC_BIN):
	$(GOCMD) build -o $(ONEBRC_BIN).exe ./$(ONEBRC_DIR)

.PHONY: test
test:
	go test -race ./...

.PHONY: audit
audit: test
	gofmt -d $(CURDIR) | colordiff
	golangci-lint run

.PHONY: no-dirty
no-dirty:
	@test -z "$(shell git status --porcelain)"