cmake_minimum_required(VERSION 3.20)

project(OpenEvent VERSION 0.3.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
include(GNUInstallDirs)

pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
pkg_check_modules(ROCKSDB REQUIRED IMPORTED_TARGET rocksdb)
pkg_check_modules(YAMLCPP REQUIRED IMPORTED_TARGET yaml-cpp)

find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

set(OPENEVENT_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/openevent-sdk/proto/openevent.proto")
set(OPENEVENT_PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${OPENEVENT_PROTO_GEN_DIR}")

set(OPENEVENT_PROTO_SRCS
    "${OPENEVENT_PROTO_GEN_DIR}/openevent.pb.cc"
    "${OPENEVENT_PROTO_GEN_DIR}/openevent.grpc.pb.cc")
set(OPENEVENT_PROTO_HDRS
    "${OPENEVENT_PROTO_GEN_DIR}/openevent.pb.h"
    "${OPENEVENT_PROTO_GEN_DIR}/openevent.grpc.pb.h")

add_custom_command(
    OUTPUT ${OPENEVENT_PROTO_SRCS} ${OPENEVENT_PROTO_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --proto_path ${CMAKE_CURRENT_SOURCE_DIR}/openevent-sdk/proto
         --cpp_out ${OPENEVENT_PROTO_GEN_DIR}
         --grpc_out ${OPENEVENT_PROTO_GEN_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
         ${OPENEVENT_PROTO}
    DEPENDS "${OPENEVENT_PROTO}"
    VERBATIM)

add_library(openevent_proto ${OPENEVENT_PROTO_SRCS})
target_include_directories(openevent_proto PUBLIC "${OPENEVENT_PROTO_GEN_DIR}")
target_link_libraries(openevent_proto PUBLIC protobuf::libprotobuf PkgConfig::GRPCPP)

add_library(openevent_storage
    src/storage/message_store.cpp
    src/storage/metadata_store.cpp
    src/storage/rocksdb_message_store.cpp)
target_include_directories(openevent_storage PUBLIC src)
target_link_libraries(openevent_storage
    PUBLIC openevent_proto
    PRIVATE PkgConfig::ROCKSDB)

add_library(openevent_service
    src/service/open_event_core.cpp
    src/service/grpc_services.cpp
    src/server/server_config.cpp)
target_include_directories(openevent_service PUBLIC src)
target_link_libraries(openevent_service
    PUBLIC openevent_storage
    PRIVATE PkgConfig::YAMLCPP PkgConfig::GRPCPP Threads::Threads)

add_executable(openevent_server src/server/main.cpp)
target_link_libraries(openevent_server PRIVATE openevent_service)

install(TARGETS openevent_server
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")

include(CTest)
if(BUILD_TESTING)
    add_executable(openevent_core_tests tests/core_tests.cpp)
    target_link_libraries(openevent_core_tests PRIVATE openevent_service)
    add_test(NAME openevent_core_tests COMMAND openevent_core_tests)
endif()
