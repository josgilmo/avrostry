GOPATH ?= $(HOME)/go
GOBIN ?= $(GOPATH)/bin

VERSION = $(shell git describe --tags --always --dirty)
BRANCH = $(shell git rev-parse --abbrev-ref HEAD)


all: help

help:
	@echo
	@echo "VERSION: $(VERSION)"
	@echo "BRANCH: $(BRANCH)"
	@echo
	@echo "usage: make <command>"
	@echo
	@echo "commands:"
	@echo "    dep       - populate vendor/ from Gopkg.lock without updating it first"
	@echo "    build     - build apps and installs them in $(GOBIN)"
	@echo "    test      - run unit tests"
	@echo "    coverage  - run unit tests and show coaverage on browser"
	@echo "    clean     - remove generated files and directories"
	@echo
	@echo "GOPATH: $(GOPATH)"
	@echo "GOBIN: $(GOBIN)"
	@echo

dep:
ifeq (, $(shell which dep))
$(error "Cannot find dep tool, install and make it available in your PATH")
endif
	@echo ">>> Ensuring dependencies..."
	@dep ensure -v

build:
	@echo ">>> Building app..."
	go install -v ./...
	@echo

test:
	@echo ">>> Running tests..."
	go test -count=1 -v ./...
	@echo

coverage:
	go test ./... -v -coverprofile=coverage.out && go tool cover -html=coverage.out

clean:
	@echo ">>> Cleaning..."
	go clean -i -r -cache -testcache
	@echo

