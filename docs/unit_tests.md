# Running Unit Tests

The unit tests code is developed with [pytest](https://docs.pytest.org/) and can be found in folder `/test`.

[Poetry](https://python-poetry.org/) manages required dependencies for running tests locally. 
Versions of the dependencies set on `pyproject.toml` should match the versions available on Databricks Runtime where the library should run on.

## Requirements:
- Python: ^3.8
- Poetry: ^1.1 (guide for Poetry install: https://python-poetry.org/docs/)

## Execution
Install dependencies: `poetry install`

Execute tests: `poetry run pytest -s`