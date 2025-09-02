PYTHON ?= python3
PIP ?= pip
MAYBE_UV = uv
PIP_COMPILE = uv pip compile

# Core paths
PACKAGES_PATH=$(PWD)/packages
PY_VENV=$(PWD)/venv
PY_VENV_DEV=$(PWD)/venv-dev
PY_VENV_REL_PATH=$(subst $(PWD)/,,$(PY_VENV))
PY_VENV_DEV_REL_PATH=$(subst $(PWD)/,,$(PY_VENV_DEV))

# Python execution
PY_PATH=$(PWD)
RUN_PY = PYTHONPATH=$(PY_PATH) $(PYTHON) -m

# Formatting and linting
PY_FIND_COMMAND = find . -name '*.py' | grep -vE "($(PY_VENV_REL_PATH))"
BLACK_CMD = $(RUN_PY) black --line-length 100 $(shell $(PY_FIND_COMMAND))
MYPY_CONFIG=$(PY_PATH)/mypy_config.ini

init:
	@if [ -d "$(PY_VENV_REL_PATH)" ]; then \
		echo "\033[33mVirtual environment already exists\033[0m"; \
	else \
		$(PYTHON) -m venv $(PY_VENV_REL_PATH); \
	fi
	@echo "\033[0;32mRun 'source $(PY_VENV_REL_PATH)/bin/activate' to activate the virtual environment\033[0m"

init_dev:
	@if [ -d "$(PY_VENV_DEV_REL_PATH)" ]; then \
		echo "\033[33mDev virtual environment already exists\033[0m"; \
	else \
		$(PYTHON) -m venv $(PY_VENV_DEV_REL_PATH); \
	fi
	@echo "\033[0;32mRun 'source $(PY_VENV_DEV_REL_PATH)/bin/activate' to activate the dev virtual environment\033[0m";


install:
	$(PIP) install --upgrade pip
	$(PIP) install uv
	$(PIP_COMPILE) --strip-extras --output-file=$(PACKAGES_PATH)/requirements.txt $(PACKAGES_PATH)/base_requirements.in
	$(MAYBE_UV) pip install -r $(PACKAGES_PATH)/requirements.txt

install_dev:
	$(PIP) install --upgrade pip
	$(PIP) install uv
	$(PIP_COMPILE) --strip-extras --output-file=$(PACKAGES_PATH)/requirements-dev.txt $(PACKAGES_PATH)/base_requirements.in $(PACKAGES_PATH)/dev_requirements.in
	$(MAYBE_UV) pip install -r $(PACKAGES_PATH)/requirements-dev.txt

format: isort
	$(RUN_PY_DIRECT) ruff check --fix $(shell $(PY_FIND_COMMAND))
	$(BLACK_CMD)

check_format_fast:
	$(RUN_PY_DIRECT) ruff check --diff $(shell $(PY_FIND_COMMAND))
	$(BLACK_CMD) --check --diff

check_format: check_format_fast
	echo "Format check complete"

mypy_mod:
	$(RUN_PY_DIRECT) mypy $(shell $(PY_MODIFIED_FIND_COMMAND)) --config-file $(MYPY_CONFIG) --namespace-packages

mypy:
	$(RUN_PY_DIRECT) mypy $(shell $(PY_FIND_COMMAND)) --config-file $(MYPY_CONFIG)

pylint_mod:
	$(RUN_PY_DIRECT) pylint $(shell $(PY_MODIFIED_FIND_COMMAND))

pylint:
	$(RUN_PY_DIRECT) pylint $(shell $(PY_FIND_COMMAND))

autopep8:
	autopep8 --in-place --aggressive --aggressive $(shell $(PY_FIND_COMMAND))

isort:
	isort $(shell $(PY_FIND_COMMAND))

lint: check_format_fast mypy_mod pylint_mod

lint_full: check_format mypy pylint

test:
	$(RUN_PY) unittest discover -s test -p *_test.py -v

upgrade: install
	$(MAYBE_UV) pip install --upgrade $$(pip freeze | awk '{split($$0, a, "=="); print a[1]}')
	$(MAYBE_UV) pip freeze > $(PACKAGES_PATH)/requirements.txt

release:
	@if [ "$(shell git rev-parse --abbrev-ref HEAD)" != "main" ]; then \
		echo "\033[0;31mERROR: You must be on the main branch to create a release.\033[0m"; \
		exit 1; \
	fi; \
	if [ ! -f VERSION ]; then \
		echo "1.0.0" > VERSION; \
		echo "\033[0;32mVERSION file not found. Created VERSION file with version 1.0.0\033[0m"; \
		VERSION_ARG="1.0.0"; \
	fi; \
	if [ -z "$(VERSION_ARG)" ]; then \
		echo "\033[0;32mCreating new version\033[0m"; \
		VERSION_ARG=$$(awk -F. '{print $$1"."$$2"."$$3+1}' VERSION); \
	fi; \
	echo "Creating version $$VERSION_ARG"; \
	echo $$VERSION_ARG > VERSION; \
	git add VERSION; \
	git commit -m "Release $$VERSION_ARG"; \
	git push; \
	git tag -l $$VERSION_ARG | grep -q $$VERSION_ARG || git tag $$VERSION_ARG; \
	git push origin $$VERSION_ARG; \
	sleep 5; \
	gh release create $$VERSION_ARG --notes "Release $$VERSION_ARG" --latest --verify-tag; \
	echo "\033[0;32mDONE!\033[0m"

clean:
	rm -rf $(PY_VENV)
	rm -rf .ruff_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf .pytest_cache
	rm -rf dist
	rm -rf .vscode


.PHONY: init install install_dev format check_format mypy pylint autopep8 isort lint test upgrade release clean
