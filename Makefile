ARCH := $(shell uname -m)

venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile .pylintrc tap_mysql/

unit_test:
	. ./venv/bin/activate ;\
	nose2 -v --with-coverage --coverage-report term -s tests/unit $(extra_args)

start_db_x86:
	docker compose -f docker-compose.x86.yml up -d
	@echo "Waiting for databases to initialize..."
	@sleep 10

# Start database for ARM architecture
start_db_arm:
	docker compose -f docker-compose.arm64.yml up -d
	@echo "Waiting for databases to initialize..."
	@sleep 10

stop_db:
	docker compose -f docker-compose.x86.yml down 2>/dev/null || true
	docker compose -f docker-compose.arm64.yml down 2>/dev/null || true

integration_test:
ifeq ($(ARCH),arm64)
	@echo "Detected ARM64 architecture. Using ARM-compatible test setup..."
	$(MAKE) integration_test_arm
else
	@echo "Detected x86 architecture. Using standard test setup..."
	$(MAKE) integration_test_x86
endif

integration_test_arm: start_db_arm
	. ./venv/bin/activate ;\
	export TAP_MYSQL_HOST=localhost ;\
	export TAP_MYSQL_PORT=3306 ;\
	export TAP_MYSQL_USER=replication_user ;\
	export TAP_MYSQL_PASSWORD=secret123passwd ;\
	export TAP_MYSQL_DATABASE=tap_mysql_test ;\
	export TAP_MYSQL_ENGINE=mariadb ;\
	nose2 -v -s tests/integration $(extra_args)

integration_test_x86: start_db_x86
	. ./venv/bin/activate ;\
	export TAP_MYSQL_HOST=localhost ;\
	export TAP_MYSQL_PORT=3306 ;\
	export TAP_MYSQL_USER=replication_user ;\
	export TAP_MYSQL_PASSWORD=secret123passwd ;\
	export TAP_MYSQL_DATABASE=tap_mysql_test ;\
	export TAP_MYSQL_ENGINE=mariadb ;\
	nose2 -v -s tests/integration $(extra_args)
