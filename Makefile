# Kage OpenD sidecar developer tasks (run from kage-opend/)

SHELL := /bin/bash
DOCKER_CONTEXT ?= default
DOCKER_ENV := DOCKER_BUILDKIT=0 COMPOSE_DOCKER_CLI_BUILD=0

COMPOSE = docker compose -f docker-compose.yml
NETWORK ?= $(or $(KAGE_DOCKER_NETWORK),kage-network)
OPEND_PORT ?= 18000

.DEFAULT_GOAL := help

.PHONY: help network build up down restart logs health shell

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

network: ## Create the shared Docker network if missing
	@DOCKER_CONTEXT=$(DOCKER_CONTEXT) docker network inspect $(NETWORK) >/dev/null 2>&1 || DOCKER_CONTEXT=$(DOCKER_CONTEXT) docker network create $(NETWORK)

build: ## Build the sidecar image
	$(COMPOSE) build

# Scope teardown to this service: all three packages share compose project `kage`,
# so a bare `compose down` would also stop the backend and the frontend.
up: network ## Rebuild and start the OpenD sidecar
	-@DOCKER_CONTEXT=$(DOCKER_CONTEXT) $(COMPOSE) rm -s -f opend
	DOCKER_CONTEXT=$(DOCKER_CONTEXT) $(DOCKER_ENV) $(COMPOSE) up -d --build

down: ## Stop the sidecar container
	DOCKER_CONTEXT=$(DOCKER_CONTEXT) $(COMPOSE) rm -s -f opend

restart: down up ## Restart the sidecar container

logs: ## Follow sidecar logs
	DOCKER_CONTEXT=$(DOCKER_CONTEXT) $(COMPOSE) logs -f --tail=100

health: ## Show sidecar status (login state + MY quote entitlement)
	DOCKER_CONTEXT=$(DOCKER_CONTEXT) $(COMPOSE) ps
	@curl -fsS --max-time 15 http://localhost:$(OPEND_PORT)/health | python3 -m json.tool || echo "sidecar -> NOT reachable on localhost:$(OPEND_PORT)"

shell: ## Open a shell in the sidecar container
	$(COMPOSE) exec opend sh
