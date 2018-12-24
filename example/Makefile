check-env:
ifndef DEBEZIUM_VERSION
export DEBEZIUM_VERSION=0.8
endif


clean: check-env
	docker-compose down || true
