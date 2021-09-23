DC=docker-compose -f docker/docker-compose.yml

.PHONY: shell
shell:
	$(DC) run --rm avro_validator_lib_test_shell

.PHONY: build
build:
	$(DC) build

.PHONY: test
test:
	$(DC) run --rm avro_validator_lib_test_shell pytest . $(args)

.PHONY: wheel
wheel:
	${DC} run --rm avro_validator_lib_test_shell python setup.py sdist bdist_wheel

.PHONY: unit-test
unit-test:
	$(DC) run --rm avro_validator_lib_test_shell pytest -m unit

.PHONY: integration-test
integration-test:
	$(DC) run --rm avro_validator_lib_test_shell pytest -m integration

.PHONY: clean
clean:
	$(DC) down --remove-orphans --volumes
	rm -Rf build/ dist/ *.egg-info/ bandit.html .pytest_cache/

.PHONY: pylint
pylint:
	$(DC) run --rm avro_validator_lib_test_shell pylint --rcfile=.pylintrc avro_validator/

.PHONY: bandit
bandit: args ?= --ignore-nosec --configfile=bandit.yaml
bandit:
	${DC} run --rm avro_validator_lib_test_shell bandit -r avro_validator/ --format html --output bandit.html ${args}