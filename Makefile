.DEFAULT_GOAL := help
.PHONY: help up down clean logs seed verify

help: ## Display this help message
	@echo "Kill It Twice Replication Platform - Operational Targets:"
	@echo "  make up       - Launch all infrastructure and services in detached mode"
	@echo "  make down     - Stop and remove running containers"
	@echo "  make clean    - Stop containers and remove persistent volume data"
	@echo "  make logs     - Follow aggregated container logs in real time"
	@echo "  make seed     - Generate high-volume synthetic transactional dataset"
	@echo "  make verify   - Run the automated 5-gate resilience verification harness"
	@echo "  make help     - Show this help message"

up: ## Launch all infrastructure and services in detached mode
	docker compose up -d

down: ## Stop running containers
	docker compose down

clean: ## Stop containers and wipe volumes
	docker compose down -v --remove-orphans

logs: ## Follow aggregated container logs
	docker compose logs -f

seed: ## Seed high-volume synthetic records
	npm run seed || node scripts/seed.js

verify: ## Run resilience verification harness
	bash ./verify.sh
