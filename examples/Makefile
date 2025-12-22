.PHONY: help _telemetry-up _telemetry-down can file kafka

help:
	@echo "Available commands:"
	@echo "  make can [up|down|server|client] - Runs CAN example"
	@echo "  make file [up|down|gen|run] - Runs file example"
	@echo "  make kafka [up|down|producer|consumer] - Runs Kafka example"

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

# File
file:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		$(MAKE) _telemetry-up; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		$(MAKE) _telemetry-down; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "gen" ]; then \
		mkdir -p file/data/in; \
		cd file && ./make_file.sh data/in/input.txt 0.1; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "run" ]; then \
		cd file && go run .; \
	else \
		echo "Usage: make file [up|down|run]"; \
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

# Catch-all target to prevent "No rule to make target" errors
%:
	@: