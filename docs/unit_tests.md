# Running Unit Tests

The unit tests code is developed with [pytest](https://docs.pytest.org/en/7.1.x/) and can be found in folder `/test`.

The dependency management is handled by [poetry](https://python-poetry.org/) in order to run the tests locally. 
Versions of the dependencies set on `pyproject.toml` should match the versions available on Databricks Runtime where the library should run on.

## Requirements:
- Python: ^3.8
- Poetry: ^1.1 (guide for Poetry install: https://python-poetry.org/docs/)

## Execution
Install dependencies: `poetry install`

Execute tests: `poetry run pytest -s`