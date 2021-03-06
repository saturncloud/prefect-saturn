
.PHONY: clean
clean:
	rm -rf ./build
	rm -rf ./dist
	rm -rf ./mypy_cache
	rm -rf ./pytest_cache

.PHONY: format
format:
	black --line-length 100 .

.PHONY: lint
lint:
	flake8 --count .
	black --check --diff .
	mypy .
	pylint prefect_saturn/

.PHONY: unit-tests
unit-tests:
	pip uninstall -y prefect-saturn || true
	python setup.py develop
	pytest --cov=prefect_saturn --cov-fail-under=100 tests/

.PHONY: test
test: clean lint unit-tests
