MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash -o pipefail -euc
.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: wasm
wasm: web/wasm_exec.js ## Build wasm
	@GOOS=js GOARCH=wasm go build -tags noasm -o web/gpq.wasm ./cmd/wasm/.

web/wasm_exec.js: ## Copy the wasm_exec.js file
	@cp "$$(go env GOROOT)/misc/wasm/wasm_exec.js" web

.PHONY: test
test: ## Run the tests
	@go test ./...

.PHONY: fixtures
fixtures: ## Run validator tests and update expected fixtures to match actuals
	@go test ./internal/validator/... >/dev/null || true
	@for f in ./internal/validator/testdata/*/actual.json; \
			do \
				cp "$$f" "$$(echo "$$f" | sed s/actual.json/expected.json/)"; \
			done;
