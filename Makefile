BUILD_DIR ?= build
BUILD_TYPE ?= Debug
JOBS ?= 2
CMAKE_ARGS ?=
PYTHON ?= python3
PREFIX ?= /usr/local

ifneq ($(filter test,$(MAKECMDGOALS)),)
override CMAKE_ARGS += -DBUILD_TESTING=ON
endif

.PHONY: build install test test-runtime

build:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) $(CMAKE_ARGS)
	cmake --build $(BUILD_DIR) --target openevent_server -j$(JOBS)

install: build
	cmake --install $(BUILD_DIR) --prefix "$(PREFIX)"

test: build
	cmake --build $(BUILD_DIR) --target openevent_core_tests -j$(JOBS)
	TMPDIR="$(abspath $(BUILD_DIR))/tmp" ctest --test-dir $(BUILD_DIR) --no-tests=error --output-on-failure

test-runtime: build
	selected_python=$$("$(PYTHON)" -E -B -c 'import sys; print(sys.executable)') && \
		$(MAKE) -C openevent-sdk test-env PYTHON="$$selected_python"
	env -u PYTHONPATH -u PYTHONHOME PYTHONDONTWRITEBYTECODE=1 OPENEVENT_SERVER_BIN="$(abspath $(BUILD_DIR))/openevent_server" "$(PYTHON)" -B -m pytest tests/test_runtime.py -o cache_dir=$(BUILD_DIR)/pytest-cache --basetemp=$(BUILD_DIR)/runtime
