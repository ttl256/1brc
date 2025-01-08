.DEFAULT_GOAL := all

BIN_NAME := weather

.PHONY: all
all: build

.PHONY: build
build:
	go build -o $(BIN_NAME) ./cmd/weather

.PHONY: test
test:
	go test -race ./...