add_library(zujan_storage STATIC
    lsm_store.cc
    io.cc
    uring_io.cc
    posix_io.cc
    memtable.cc
    wal.cc
    sstable.cc
    arena.cc
    coding.cc
    hash.cc
    bloom_filter.cc
    cache.cc
    block.cc
    block_builder.cc
    filter_block.cc
    table_builder.cc
)

target_include_directories(zujan_storage PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(zujan_storage PUBLIC PkgConfig::uring)
