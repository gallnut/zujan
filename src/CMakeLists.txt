add_subdirectory(storage)
add_subdirectory(consensus)

add_executable(zujan main.cc)

target_include_directories(zujan PRIVATE ${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(zujan PRIVATE zujan_storage zujan_raft)