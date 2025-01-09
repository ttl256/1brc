.DEFAULT_GOAL := all

PACKAGE_NAME ?= weather
BIN_EXTENSION ?= exe
BIN_NAME := $(PACKAGE_NAME).$(BIN_EXTENSION)

.PHONY: all
all: build

.PHONY: build
build:
	go build -o $(BIN_NAME) ./cmd/$(PACKAGE_NAME)

.PHONY: test
test:
	go test -race ./...