# googletest
CPMAddPackage(
    NAME googletest
    GITHUB_REPOSITORY google/googletest
    GIT_TAG v1.17.0
    VERSION 1.17.0
    OPTIONS "INSTALL_GTEST OFF" "gtest_force_shared_crt ON"
)

# gRPC
find_package(gRPC CONFIG REQUIRED)

# protobuf
find_package(Protobuf CONFIG REQUIRED)

# liburing (System installed)
find_package(PkgConfig REQUIRED)
pkg_check_modules(uring REQUIRED liburing IMPORTED_TARGET)

# google/benchmark
CPMAddPackage(
    NAME benchmark
    GITHUB_REPOSITORY google/benchmark
    GIT_TAG v1.9.0
    VERSION 1.9.0
    OPTIONS "BENCHMARK_ENABLE_TESTING OFF" "BENCHMARK_ENABLE_INSTALL OFF"
)