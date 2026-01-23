.PHONY: up down clean network airflow-up airflow-down all-up all-down

network:
	@docker network create bigdata_network 2>/dev/null || echo "Network already exists"

up: network
	docker-compose up -d

down:
	docker-compose down

airflow-up:
	docker-compose -f docker-compose-airflow.yml up -d

airflow-down:
	docker-compose -f docker-compose-airflow.yml down

all-up: network up airflow-up

all-down: airflow-down down

clean:
	docker-compose down -v
	docker-compose -f docker-compose-airflow.yml down -v