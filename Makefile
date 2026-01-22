INFRA = docker-compose-infra.yml
SPARK = docker-compose-spark.yml
WORK  = docker-compose-work.yml
NETWORK = bigdata_network

.PHONY: up down clean restart

up:
	@docker network create $(NETWORK) 2>nul || cd .
	docker-compose -f $(INFRA) up -d
	docker-compose -f $(SPARK) up -d
	docker-compose -f $(WORK) up -d

down:
	docker-compose -f $(WORK) down
	docker-compose -f $(SPARK) down
	docker-compose -f $(INFRA) down

clean:
	docker-compose -f $(WORK) down -v
	docker-compose -f $(SPARK) down -v
	docker-compose -f $(INFRA) down -v

restart: clean up