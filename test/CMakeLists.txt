include(GoogleTest)

add_executable(main_test main_test.cc lsm_test.cc raft_log_test.cc raft_meta_test.cc)
target_link_libraries(main_test PRIVATE gtest_main zujan_storage zujan_raft zujan_proto)
target_include_directories(main_test PRIVATE ${PROJECT_SOURCE_DIR}/src)

add_executable(raft_node_main raft_node_main.cc)
target_link_libraries(raft_node_main PRIVATE zujan_raft zujan_storage zujan_proto)
target_include_directories(raft_node_main PRIVATE ${PROJECT_SOURCE_DIR}/src)

add_executable(benchmark_test benchmark.cc)
target_link_libraries(benchmark_test PRIVATE benchmark::benchmark zujan_storage pthread)
target_include_directories(benchmark_test PRIVATE ${PROJECT_SOURCE_DIR}/src)

gtest_discover_tests(main_test)