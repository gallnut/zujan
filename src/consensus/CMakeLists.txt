add_library(zujan_raft STATIC
    raft.cc
)

target_include_directories(zujan_raft PUBLIC ${PROJECT_SOURCE_DIR}/src)
# Will link zujan_proto when Protobuf compilation is configured
# target_link_libraries(zujan_raft PUBLIC zujan_proto)
