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

SRC_PATH=$(PWD)/src
TEST_PATH=$(PWD)/test
BUILD_PATH=$(PWD)/build
TYPES_PATH=$(PWD)/data_types

PB_PY_PATH=$(BUILD_PATH)/pb_types
PB_DESC_PATH=$(BUILD_PATH)/pb_desc
PB_SRC_PATH=$(SRC_PATH)/ry_pg_utils/pb_types

# Python execution
PY_PATH=$(SRC_PATH):$(TEST_PATH):$(BUILD_PATH)

RUN_PY = PYTHONPATH=$(PY_PATH) $(PYTHON) -m
RUN_PY_DIRECT = PYTHONPATH=$(PY_PATH) $(PYTHON)

# Formatting and linting
PY_FIND_COMMAND = find . -name '*.py' | grep -vE "($(PY_VENV_REL_PATH)|$(PY_VENV_DEV_REL_PATH))"
PY_MODIFIED_FIND_COMMAND = git diff --name-only --diff-filter=AM HEAD | grep '\.py$$' | grep -vE "($(PY_VENV_REL_PATH)|$(PY_VENV_DEV_REL_PATH))"
BLACK_CMD = $(RUN_PY) black --line-length 100 $(shell $(PY_FIND_COMMAND))
MYPY_CONFIG=$(PY_PATH)/mypy_config.ini

PROTO_PATH=$(TYPES_PATH)/proto
PROTO_FIND_COMMAND=`find $(PROTO_PATH) -type f -name '*.proto'`


proto_build:
	@echo "\033[34mBuilding proto files\033[0m"
	mkdir -p $(PB_PY_PATH)
	touch $(PB_PY_PATH)/__init__.py
	touch $(PB_PY_PATH)/py.typed
	protoc \
		-I=$(PROTO_PATH) \
		-I/usr/include \
		--python_out=$(PB_PY_PATH) \
		--mypy_out=$(PB_PY_PATH) \
		$(PROTO_FIND_COMMAND)
	# Fix imports in generated files
	find $(PB_PY_PATH) -name "*_pb2.py" \
		-exec sed -i \
		's/import \([a-zA-Z0-9_]*\)_pb2 as \1__pb2/from pb_types import \1_pb2 as \1__pb2/g' \
		{} \;
	@echo "\033[32mProto files built successfully\033[0m"

proto_desc:
	@echo "\033[34mDescribing proto files\033[0m"
	mkdir -p $(PB_DESC_PATH)
	protoc -I=$(PROTO_PATH) --descriptor_set_out=$(PB_DESC_PATH)/types.desc --include_imports $(PROTO_FIND_COMMAND)
	@echo "\033[32mProto files described successfully\033[0m"

proto_clean:
	@echo "\033[34mCleaning proto files\033[0m"
	rm -rf $(PB_PY_PATH)

types_build: proto_build
	@echo "\033[32mTypes built successfully\033[0m"

types_clean: proto_clean
	@echo "\033[34mCleaning types\033[0m"

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

copy_proto:
	@if [ -d "$(PB_PY_PATH)" ]; then \
		echo "\033[0;32mCopying generated protobuf files from build to src...\033[0m"; \
		mkdir -p $(PB_SRC_PATH); \
		cp -r $(PB_PY_PATH)/* $(PB_SRC_PATH)/; \
		echo "\033[0;32mProtobuf files copied to $(PB_SRC_PATH)\033[0m"; \
	else \
		echo "\033[33mNo protobuf files found in $(PB_PY_PATH)\033[0m"; \
	fi

format: isort
	$(RUN_PY) ruff check --fix $(shell $(PY_FIND_COMMAND))
	$(BLACK_CMD)

check_format_fast:
	$(RUN_PY) ruff check --diff $(shell $(PY_FIND_COMMAND))
	$(BLACK_CMD) --check --diff

check_format: check_format_fast
	echo "Format check complete"

mypy_mod:
	@MODIFIED_FILES=$$($(PY_MODIFIED_FIND_COMMAND)); \
	if [ -n "$$MODIFIED_FILES" ]; then \
		$(RUN_PY) mypy $$MODIFIED_FILES --config-file $(MYPY_CONFIG) --namespace-packages; \
	else \
		echo "No modified Python files to check with mypy"; \
	fi

mypy:
	$(RUN_PY) mypy $(shell $(PY_FIND_COMMAND)) --config-file $(MYPY_CONFIG)

pylint_mod:
	@MODIFIED_FILES=$$($(PY_MODIFIED_FIND_COMMAND)); \
	if [ -n "$$MODIFIED_FILES" ]; then \
		$(RUN_PY) pylint $$MODIFIED_FILES; \
	else \
		echo "No modified Python files to check with pylint"; \
	fi

pylint:
	$(RUN_PY) pylint $(shell $(PY_FIND_COMMAND))

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

release: copy_proto
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
	rm -rf $(PY_VENV_DEV)
	rm -rf $(BUILD_PATH)
	rm -rf .ruff_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf .pytest_cache
	rm -rf dist


.PHONY: init install install_dev copy_proto format check_format \
        mypy pylint autopep8 isort lint test upgrade release clean
