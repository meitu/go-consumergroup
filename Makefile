export GO15VENDOREXPERIMENT=1

# Many Go tools take file globs or directories as arguments instead of packages.
PKG_FILES ?= *.go

# The linting tools evolve with each Go version, so run them only on the latest
# stable release.
GO_VERSION := $(shell go version | cut -d " " -f 3)
GO_MINOR_VERSION := $(word 2,$(subst ., ,$(GO_VERSION)))
ifneq ($(filter $(LINTABLE_MINOR_VERSIONS),$(GO_MINOR_VERSION)),)
	SHOULD_LINT := true
endif

.PHONY: build
build:
	go build

.PHONY: lint
lint:
	@rm -rf lint.log
	@echo "Checking formatting..."
	@gofmt -d -s $(PKG_FILES) 2>&1 | tee lint.log
	@echo "Installing test dependencies for vet..."
	@go test -i $(PKGS)
	@echo "Checking vet..."
	@go tool vet $(PKG_FILES) 2>&1 | tee -a lint.log;
	@echo "Checking lint..."
	@golint $(PKG_FILES) 2>&1 | tee -a lint.log;
	@echo "Checking for unresolved FIXMEs..."
	@git grep -i fixme | grep -v -e vendor -e Makefile | tee -a lint.log
	@[ ! -s lint.log ]

.PHONY: test
test:
	cd tests && sh test.sh
