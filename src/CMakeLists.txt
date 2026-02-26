# Generate protobuf and grpc code
set(PROTO_SRC "${PROJECT_SOURCE_DIR}/proto/zujan.proto")
set(PROTO_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${PROTO_OUT_DIR})

# Use CMake's built-in protobuf generation if available, or custom command
find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

add_custom_command(
    OUTPUT
        "${PROTO_OUT_DIR}/zujan.pb.cc"
        "${PROTO_OUT_DIR}/zujan.pb.h"
        "${PROTO_OUT_DIR}/zujan.grpc.pb.cc"
        "${PROTO_OUT_DIR}/zujan.grpc.pb.h"
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS --cpp_out="${PROTO_OUT_DIR}"
         --grpc_out="${PROTO_OUT_DIR}"
         --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
         -I "${PROJECT_SOURCE_DIR}/proto"
         "${PROTO_SRC}"
    DEPENDS "${PROTO_SRC}"
)

add_library(zujan_proto STATIC
    "${PROTO_OUT_DIR}/zujan.pb.cc"
    "${PROTO_OUT_DIR}/zujan.grpc.pb.cc"
)
target_include_directories(zujan_proto PUBLIC "${PROTO_OUT_DIR}" "${PROJECT_SOURCE_DIR}/proto")
target_link_libraries(zujan_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_subdirectory(utils)
add_subdirectory(storage)
add_subdirectory(consensus)

add_executable(zujan main.cc)

target_include_directories(zujan PRIVATE ${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}/include "${PROTO_OUT_DIR}")
target_link_libraries(zujan PRIVATE zujan_raft zujan_storage zujan_proto zujan_utils)