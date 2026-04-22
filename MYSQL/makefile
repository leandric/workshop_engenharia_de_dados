PROJECT_NAME = mysql_dw_bi

MYSQL_ROOT_PASSWORD = 12F95D9BB576440A
MYSQL_DATABASE = DW_BI
MYSQL_USER = app_bi
MYSQL_PASSWORD = 12F95D9BB576440A
MYSQL_PORT = 3306

# Volume nomeado para persistir os dados
MYSQL_VOLUME = $(PROJECT_NAME)_data

.PHONY: up down logs restart clean purge

up:
	# garante que o volume exista (não dá erro se já existir)
	docker volume create $(MYSQL_VOLUME) >/dev/null

	docker run -d --name $(PROJECT_NAME) \
		-e MYSQL_ROOT_PASSWORD=$(MYSQL_ROOT_PASSWORD) \
		-e MYSQL_DATABASE=$(MYSQL_DATABASE) \
		-e MYSQL_USER=$(MYSQL_USER) \
		-e MYSQL_PASSWORD=$(MYSQL_PASSWORD) \
		-p $(MYSQL_PORT):3306 \
		-v $(MYSQL_VOLUME):/var/lib/mysql \
		--restart always \
		mysql:8.0

down:
	docker stop $(PROJECT_NAME) || true
	docker rm $(PROJECT_NAME) || true

logs:
	docker logs -f $(PROJECT_NAME)

restart: down up

# "clean" agora só remove container (mantém dados)
clean: down

# apaga TUDO (container + volume) => perde os dados
purge: down
	docker volume rm $(MYSQL_VOLUME) || true