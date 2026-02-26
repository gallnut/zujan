add_library(zujan_utils STATIC
    logger.cc
)

target_include_directories(zujan_utils PUBLIC ${PROJECT_SOURCE_DIR}/src)
