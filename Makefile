DOCKER_COMPOSE_VERSION := 1.29.2
DOCKER_COMPOSE_PATH := .tools/docker-compose
DOCKER_COMPOSE_FILE := docker-compose/tools.yaml

.PHONY: all build run

all: build run

$(DOCKER_COMPOSE_PATH):
	mkdir -p .tools
	curl -L "https://github.com/docker/compose/releases/download/$(DOCKER_COMPOSE_VERSION)/docker-compose-$(shell uname -s)-$(shell uname -m)" -o $(DOCKER_COMPOSE_PATH)
	chmod +x $(DOCKER_COMPOSE_PATH)

build: $(DOCKER_COMPOSE_PATH)
	$(DOCKER_COMPOSE_PATH) -f $(DOCKER_COMPOSE_FILE) build --no-cache

run: build
	$(DOCKER_COMPOSE_PATH) -f $(DOCKER_COMPOSE_FILE) run --rm app
