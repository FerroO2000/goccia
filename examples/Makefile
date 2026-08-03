.PHONY: help _telemetry-up _telemetry-down can csv ebpf file http kafka routing

help:
	@echo "Available commands:"
	@echo "  make can [up|down|server|client] - Runs CAN example"
	@echo "  make csv [up|down|run] - Runs CSV example"
	@echo "  make ebpf [up|down|build|run] - Runs eBPF example"
	@echo "  make file [up|down|gen|run] - Runs file example"
	@echo "  make http [up|down|server|client [period]] - Runs HTTP echo example"
	@echo "  make kafka [up|down|producer|consumer] - Runs Kafka example"
	@echo "  make routing [up|down|run] - Runs routing example"

_telemetry-up:
	@cd telemetry && docker compose up -d

_telemetry-down:
	@cd telemetry && docker compose down

# CAN
can:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
		cd can && docker compose up -d; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
		cd can && docker compose down; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "server" ]; then \
		cd can/server && go run .; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "client" ]; then \
		cd can/client && go run .; \
	else \
		echo "Usage: make can [up|down|server|client]"; \
		exit 1; \
	fi

# CSV
csv:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "run" ]; then \
		cd csv && go run .; \
	else \
		echo "Usage: make csv [up|down|run]"; \
		exit 1; \
	fi

# eBPF
ebpf:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "build" ]; then \
		cd ebpf && go build -o build/; \
	elif echo "$(filter-out $@,$(MAKECMDGOALS))" | grep -q "^run"; then \
		INTERFACE=$$(echo "$(filter-out $@,$(MAKECMDGOALS))" | awk '{print $$2}'); \
		if [ -z "$$INTERFACE" ]; then \
			INTERFACE="eth0"; \
		fi; \
		cd ebpf && ./build/ebpf "$$INTERFACE"; \
	else \
		echo "Usage: make ebpf [up|down|build|run [interface]]"; \
		exit 1; \
	fi

# File
file:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
	elif echo "$(filter-out $@,$(MAKECMDGOALS))" | grep -q "^gen"; then \
		mkdir -p file/data/in; \
		FILE=$$(echo "$(filter-out $@,$(MAKECMDGOALS))" | awk '{print $$2}'); \
		if [ -z "$$FILE" ]; then \
			FILE="input.txt"; \
		fi; \
		cd file && ./gen_file.sh data/in/"$$FILE" 0.1; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "run" ]; then \
		cd file && go run .; \
	else \
		echo "Usage: make file [up|down|run]"; \
		exit 1; \
	fi

# HTTP
http:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "server" ]; then \
		cd http && go run .; \
	elif echo "$(filter-out $@,$(MAKECMDGOALS))" | grep -q "^client"; then \
		PERIOD=$$(echo "$(filter-out $@,$(MAKECMDGOALS))" | awk '{print $$2}'); \
		if [ -z "$$PERIOD" ]; then \
			PERIOD="1"; \
		fi; \
		cd http && ./client.sh "$$PERIOD"; \
	else \
		echo "Usage: make http [up|down|server|client [period]]"; \
		exit 1; \
	fi

# Kafka
kafka:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
		cd kafka && docker compose up -d; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
		cd kafka && docker compose down; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "producer" ]; then \
		cd kafka/producer && go run .; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "consumer" ]; then \
		cd kafka/consumer && go run .; \
	else \
		echo "Usage: make kafka [up|down|producer|consumer]"; \
		exit 1; \
	fi

# Routing
routing:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "run" ]; then \
		cd routing && go run .; \
	else \
		echo "Usage: make routing [up|down|run]"; \
		exit 1; \
	fi

# Catch-all target to prevent "No rule to make target" errors
%:
	@:
