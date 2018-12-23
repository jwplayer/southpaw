check-env:
ifndef DEBEZIUM_VERSION
export DEBEZIUM_VERSION=0.8
endif

.PHONY: run
run: clean check-env
	docker-compose up --build -d
	@echo "wait 10s for services to start before re-running southpaw"
	sleep 10
	docker-compose up -d southpaw
	docker-compose logs -f -t

clean: check-env
	docker-compose kill
	docker-compose down || tru
