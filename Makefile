# Makefile for Python development & CI
# You need: black, flake8, pylint.
# test: lint unit-tests

lint: black-ci flake8 pylint-shorter

env:
	python3 -m venv venv
	echo "run: source ./venv/bin/activate"

install:
	pip install --upgrade pip
	pip install -r requirements.txt

black:
	black --line-length 99 .

black-ci:
	echo -e "\n# Diff for each file:"; \
	black --line-length 99 --diff .; \
	echo -e "\n# Status:"; \
	black --line-length 99 --check .

flake8:
	flake8 --extend-exclude venv,build

PYLINT_FILES = `find . \
		-path './docs' -prune -o \
		-path './venv' -prune -o \
		-path './build' -prune -o \
		-name '*.py' -print`;

pylint:
	python3 -m pylint $(PYLINT_FILES)

pylint-shorter:
	python3 -m pylint --enable=useless-suppression $(PYLINT_FILES)

# unit-tests:
# 	python3 -m pytest -rxXs --cov
