INFRA   = docker-compose-infra.yml
SPARK   = docker-compose-spark.yml
WORK    = docker-compose-work.yml
AIRFLOW = docker-compose-airflow.yml
NETWORK = bigdata_network

.PHONY: up down clean restart rebuild-all

up:
	@docker network create $(NETWORK) 2>/dev/null || true
	docker-compose -f $(INFRA) up -d
	docker-compose -f $(SPARK) up -d
	docker-compose -f $(WORK) up -d
	docker-compose -f $(AIRFLOW) up -d

down:
	docker-compose -f $(AIRFLOW) down
	docker-compose -f $(WORK) down
	docker-compose -f $(SPARK) down
	docker-compose -f $(INFRA) down

clean:
	docker-compose -f $(AIRFLOW) down -v
	docker-compose -f $(WORK) down -v
	docker-compose -f $(SPARK) down -v
	docker-compose -f $(INFRA) down -v

restart: clean up

rebuild-all:
	docker-compose -f $(AIRFLOW) down
	docker-compose -f $(WORK) down
	docker-compose -f $(SPARK) down
	docker-compose -f $(INFRA) down
	docker-compose -f $(INFRA) build --no-cache
	docker-compose -f $(SPARK) build --no-cache
	docker-compose -f $(WORK) build --no-cache
	docker-compose -f $(AIRFLOW) build --no-cache
	docker-compose -f $(INFRA) up -d
	docker-compose -f $(SPARK) up -d
	docker-compose -f $(WORK) up -d
	docker-compose -f $(AIRFLOW) up -d