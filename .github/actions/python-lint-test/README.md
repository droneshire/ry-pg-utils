# Python Lint and Test Action

A composite GitHub Action for running Python linting and tests with coverage reporting.

## Features

- 🚀 **Fast**: Uses composite action (no Docker build time)
- 📦 **Modern**: Uses `uv` for fast dependency installation
- 🔍 **Comprehensive**: Runs black, mypy, pylint, and pytest
- 📊 **Coverage**: Automatically extracts and reports code coverage
- 🐍 **Flexible**: Configurable Python version

## Usage

```yaml
- name: Run linting and tests
  id: lint-test
  uses: ./.github/actions/python-lint-test
  with:
    python-version: '3.12'  # Optional, defaults to 3.12
    version: 'v1.0.0'       # Optional, defaults to v1.0.0

- name: Display results
  run: |
    echo "${{ steps.lint-test.outputs.result }}"
    echo "Coverage: ${{ steps.lint-test.outputs.coverage_percentage }}"
```

## Inputs

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `python-version` | Python version to use | No | `3.12` |
| `version` | App version (informational) | No | `v1.0.0` |

## Outputs

| Output | Description |
|--------|-------------|
| `result` | Success message if all checks pass |
| `coverage_percentage` | Code coverage percentage (e.g., "95%") |

## What it does

1. Sets up Python with pip caching
2. Installs dependencies using `uv`
3. Runs formatting check with `black` and `ruff`
4. Runs type checking with `mypy`
5. Runs linting with `pylint`
6. Runs tests with `pytest`
7. Extracts code coverage percentage

## Requirements

- Requires `Makefile` with targets: `check_format`, `mypy`, `pylint`, `test`
- Requires dependency files: `packages/base_requirements.in`, `packages/dev_requirements.in`
- Uses `testcontainers` for integration tests (Docker must be available on runner)

## Migration from Docker Action

This action was previously a Docker-based action. The composite version provides:
- ⚡ **Faster execution** (no Docker build step)
- 🔄 **Better caching** (pip cache between runs)
- 🛠️ **Easier maintenance** (pure YAML configuration)
- 📦 **Smaller footprint** (no custom Docker image)
