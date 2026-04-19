.PHONY: up down restart logs shell clean reset

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d --build

logs:
	docker compose logs -f sftp

shell:
	docker exec -it sftp-server sh

clean:
	docker compose down -v --remove-orphans

reset: clean up