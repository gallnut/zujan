add_library(zujan_raft STATIC
    raft.cc
    raft_log.cc
    raft_meta.cc
    raft_service.cc
)

target_include_directories(zujan_raft PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(zujan_raft PUBLIC zujan_proto)
