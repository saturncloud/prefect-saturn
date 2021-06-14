
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
	pip install types-requests
	flake8 --count .
	black --check --diff .
	mypy .
	pylint prefect_saturn/

.PHONY: unit-tests
unit-tests:
	pip install --upgrade .
	pytest --cov --cov-fail-under=88 tests/

.PHONY: test
test: clean lint unit-tests
