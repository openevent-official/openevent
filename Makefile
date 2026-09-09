BUILD_DIR ?= build
.DEFAULT_GOAL := build
BUILD_TYPE ?= Debug
JOBS ?= 2
CMAKE_ARGS ?=
PYTHON ?= python3

.PHONY: build configure test test-runtime check-docs

configure:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) $(CMAKE_ARGS)

build: configure
	cmake --build $(BUILD_DIR) -j$(JOBS)

test: build
	TMPDIR="$(abspath $(BUILD_DIR))/tmp" ctest --test-dir $(BUILD_DIR) --output-on-failure

test-runtime: build
	PYTHONDONTWRITEBYTECODE=1 OPENEVENT_SERVER_BIN="$(abspath $(BUILD_DIR))/openevent_server" "$(PYTHON)" -m pytest tests/test_runtime.py -o cache_dir=$(BUILD_DIR)/pytest-cache --basetemp=$(BUILD_DIR)/runtime

check-docs:
	PYTHONDONTWRITEBYTECODE=1 "$(PYTHON)" tests/check_docs.py $(DOCS_ROOT)
