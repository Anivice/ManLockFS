cmake_minimum_required(VERSION 3.20)
project(MLFS CXX C)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_C_STANDARD 23)

add_executable(mount.mlfs mount.cpp)

# Add libfuse
include(FindPkgConfig)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBFUSE REQUIRED fuse3)
target_compile_definitions(mount.mlfs PRIVATE "_FILE_OFFSET_BITS=64" "CFS_COMPILE_FUSE=1" "__USE_FILE_OFFSET64=1")
target_include_directories(mount.mlfs PRIVATE ${LIBFUSE_INCLUDE_DIRS})
target_compile_options(mount.mlfs PRIVATE ${LIBFUSE_CFLAGS_OTHER})
target_link_libraries(mount.mlfs PRIVATE ${LIBFUSE_LIBRARIES})
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    target_compile_definitions(mount.mlfs PRIVATE DEBUG=1)
    target_compile_options(mount.mlfs PRIVATE -fsanitize=thread -fno-sanitize-recover=undefined -fno-sanitize=undefined)
    target_link_libraries(mount.mlfs PRIVATE tsan)
else ()
    target_compile_definitions(mount.mlfs PRIVATE DEBUG=0)
endif ()

add_executable(lock lock.c)
