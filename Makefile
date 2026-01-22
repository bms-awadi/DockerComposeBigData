.PHONY: up down clean network

network:
	@docker network create bigdata_network 2>/dev/null || echo "Network already exists"

up: network
	docker-compose up -d

down:
	docker-compose down

clean:
	docker-compose down -v
	@docker network rm bigdata_network 2>/dev/null || echo "Network already removed"