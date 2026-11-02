cmake_minimum_required(VERSION 3.16)
project(nexus LANGUAGES CXX VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2")
add_compile_options(-Wall -Wextra -Wno-unused-parameter)

find_package(Threads REQUIRED)

add_library(nexus_core STATIC
  src/net/fabric.cpp
  src/net/transport.cpp
  src/core/scheduler.cpp
  src/node/node.cpp
  src/node/telemetry.cpp
  src/node/home.cpp
  src/node/executor.cpp
)
target_include_directories(nexus_core PUBLIC include)
target_link_libraries(nexus_core PUBLIC Threads::Threads rt)

add_executable(nexus_node src/node/main.cpp)
target_link_libraries(nexus_node PRIVATE nexus_core)

add_executable(nexus_launcher
  src/launcher/main.cpp
  src/launcher/sysinfo.cpp
  src/launcher/workload.cpp
  src/launcher/experiment.cpp
  src/launcher/http.cpp
)
target_link_libraries(nexus_launcher PRIVATE nexus_core)
target_compile_definitions(nexus_launcher PRIVATE NEXUS_SOURCE_DIR="${CMAKE_SOURCE_DIR}")

enable_testing()
add_executable(nexus_tests tests/test_main.cpp src/launcher/workload.cpp)
target_link_libraries(nexus_tests PRIVATE nexus_core)
target_compile_definitions(nexus_tests PRIVATE NEXUS_TEST_CONFIG="${CMAKE_SOURCE_DIR}/config/cluster.json")
add_test(NAME unit COMMAND nexus_tests)
