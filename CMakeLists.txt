cmake_minimum_required(VERSION 3.20)

project(OpenEvent VERSION 0.13.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
include(GNUInstallDirs)

pkg_check_modules(SERVER_DEPS REQUIRED grpc++ protobuf rocksdb yaml-cpp openssl)
set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
set(server_link_flags ${SERVER_DEPS_STATIC_LDFLAGS})

# Some distributions name the static c-ares archive libcares_static.a.
if("-lcares" IN_LIST server_link_flags)
    find_library(CARES_ARCHIVE NAMES cares cares_static HINTS ${SERVER_DEPS_STATIC_LIBRARY_DIRS} REQUIRED)
    list(TRANSFORM server_link_flags REPLACE "^-lcares$" "${CARES_ARCHIVE}")
endif()

# Some rocksdb.pc files omit the optional compression and flags libraries.
# The linker pulls code from these archives only when RocksDB references it.
foreach(lib IN ITEMS snappy z bz2 lz4 zstd gflags)
    find_library(ROCKSDB_${lib}_ARCHIVE ${lib} HINTS ${SERVER_DEPS_STATIC_LIBRARY_DIRS})
    if(ROCKSDB_${lib}_ARCHIVE)
        list(APPEND server_link_flags "${ROCKSDB_${lib}_ARCHIVE}")
    endif()
endforeach()

add_library(openevent_dependencies INTERFACE)
target_include_directories(openevent_dependencies SYSTEM INTERFACE ${SERVER_DEPS_STATIC_INCLUDE_DIRS})
target_compile_options(openevent_dependencies INTERFACE ${SERVER_DEPS_STATIC_CFLAGS_OTHER})
target_link_options(openevent_dependencies INTERFACE -static)
target_link_libraries(openevent_dependencies INTERFACE
    -Wl,--start-group ${server_link_flags} -Wl,--end-group)

find_program(Protobuf_PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

set(OPENEVENT_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/openevent-sdk/proto")
set(OPENEVENT_PROTO_PATH "openevent/sdk/proto")
set(OPENEVENT_PROTO_FILES
    "${OPENEVENT_PROTO_ROOT}/${OPENEVENT_PROTO_PATH}/openevent.proto"
    "${OPENEVENT_PROTO_ROOT}/${OPENEVENT_PROTO_PATH}/admin.proto")
set(OPENEVENT_PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(OPENEVENT_PROTO_OUTPUT "${OPENEVENT_PROTO_GEN_DIR}/${OPENEVENT_PROTO_PATH}")
file(MAKE_DIRECTORY "${OPENEVENT_PROTO_GEN_DIR}")

set(OPENEVENT_PROTO_SRCS
    "${OPENEVENT_PROTO_OUTPUT}/openevent.pb.cc"
    "${OPENEVENT_PROTO_OUTPUT}/openevent.grpc.pb.cc"
    "${OPENEVENT_PROTO_OUTPUT}/admin.pb.cc"
    "${OPENEVENT_PROTO_OUTPUT}/admin.grpc.pb.cc")
set(OPENEVENT_PROTO_HDRS
    "${OPENEVENT_PROTO_OUTPUT}/openevent.pb.h"
    "${OPENEVENT_PROTO_OUTPUT}/openevent.grpc.pb.h"
    "${OPENEVENT_PROTO_OUTPUT}/admin.pb.h"
    "${OPENEVENT_PROTO_OUTPUT}/admin.grpc.pb.h")

add_custom_command(
    OUTPUT ${OPENEVENT_PROTO_SRCS} ${OPENEVENT_PROTO_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --proto_path ${OPENEVENT_PROTO_ROOT}
         --cpp_out ${OPENEVENT_PROTO_GEN_DIR}
         --grpc_out ${OPENEVENT_PROTO_GEN_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         ${OPENEVENT_PROTO_FILES}
    DEPENDS ${OPENEVENT_PROTO_FILES}
    VERBATIM)

add_library(openevent_proto STATIC ${OPENEVENT_PROTO_SRCS})
target_include_directories(openevent_proto PUBLIC "${OPENEVENT_PROTO_GEN_DIR}")
target_link_libraries(openevent_proto PUBLIC openevent_dependencies)

set(OPENEVENT_STORAGE_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/src/storage/object_record.proto")
set(OPENEVENT_STORAGE_PROTO_SRC "${OPENEVENT_PROTO_GEN_DIR}/object_record.pb.cc")
set(OPENEVENT_STORAGE_PROTO_HDR "${OPENEVENT_PROTO_GEN_DIR}/object_record.pb.h")
add_custom_command(
    OUTPUT ${OPENEVENT_STORAGE_PROTO_SRC} ${OPENEVENT_STORAGE_PROTO_HDR}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --proto_path ${CMAKE_CURRENT_SOURCE_DIR}/src/storage
         --cpp_out ${OPENEVENT_PROTO_GEN_DIR}
         ${OPENEVENT_STORAGE_PROTO}
    DEPENDS ${OPENEVENT_STORAGE_PROTO}
    VERBATIM)
add_library(openevent_storage_proto STATIC ${OPENEVENT_STORAGE_PROTO_SRC})
target_include_directories(openevent_storage_proto PUBLIC "${OPENEVENT_PROTO_GEN_DIR}")
target_link_libraries(openevent_storage_proto PUBLIC openevent_dependencies)

add_library(openevent_storage STATIC
    src/storage/object_file_store.cpp
    src/storage/unified_storage.cpp)
target_include_directories(openevent_storage PUBLIC src)
target_link_libraries(openevent_storage PUBLIC openevent_proto openevent_storage_proto)

add_library(openevent_service STATIC
    src/service/open_event_core.cpp
    src/service/grpc_services.cpp
    src/server/server_config.cpp)
target_include_directories(openevent_service PUBLIC src)
target_link_libraries(openevent_service PUBLIC openevent_storage)

add_executable(openevent_server src/server/main.cpp)
target_link_libraries(openevent_server PRIVATE openevent_service)

install(TARGETS openevent_server
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")

include(CTest)
if(BUILD_TESTING)
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tmp")
    add_executable(openevent_core_tests tests/core_tests.cpp)
    target_link_libraries(openevent_core_tests PRIVATE openevent_service)
    add_test(NAME openevent_core_tests COMMAND openevent_core_tests)
    set_tests_properties(openevent_core_tests PROPERTIES TIMEOUT 300)
    add_test(NAME openevent_server_static
        COMMAND "${CMAKE_READELF}" --program-headers --dynamic "$<TARGET_FILE:openevent_server>")
    set_tests_properties(openevent_server_static PROPERTIES FAIL_REGULAR_EXPRESSION "INTERP;\\(NEEDED\\)")
endif()
