# Test dependencies
find_package(GTest REQUIRED)
find_package(benchmark REQUIRED)
find_package(Threads REQUIRED)

  # Test target
  add_executable(dagforge_tests
    test_main.cpp
    test_utils.hpp

  # Core tests
    runtime_test.cpp
  shard_affinity_test.cpp
  coroutine_test.cpp
  shard_test.cpp

  # Scheduler tests
  engine_test.cpp
  cron_test.cpp

  # DAG tests
  dag_test.cpp
  dag_manager_test.cpp
  dag_domain_test.cpp
  dag_run_test.cpp
  trigger_rule_test.cpp
  branching_test.cpp

  # Storage tests
  persistence_test.cpp
  config_test.cpp
  config_watcher_test.cpp
  config_loader_fuzz_test.cpp
  dag_validation_test.cpp

  # Executor tests
  executor_test.cpp
  docker_executor_test.cpp
  sensor_executor_test.cpp

  # HTTP/Docker client tests
  http_client_test.cpp
  docker_client_test.cpp

  # I/O tests
  io_bridge_test.cpp

  # XCom tests
  xcom_test.cpp
  template_resolver_test.cpp

  # API tests
  api_test.cpp
  cli_test.cpp
  cli_binary_test.cpp
  websocket_test.cpp

  # Integration tests
  integration_test.cpp
  api_e2e_test.cpp
)

target_link_libraries(dagforge_tests
  PRIVATE
    DAGForge::lib
    GTest::GTest
    Threads::Threads
    OpenSSL::Crypto
)

target_include_directories(dagforge_tests
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_definitions(dagforge_tests
  PRIVATE
    DAGFORGE_BIN_PATH="$<TARGET_FILE:dagforge>"
)

# CLIBinary tests execute the real dagforge binary.
# Keep it up to date whenever dagforge_tests is built.
add_dependencies(dagforge_tests dagforge)

dagforge_configure_target(dagforge_tests)

# Register tests with CTest
include(GoogleTest)
# All non-persistence tests: discovered individually, run in parallel.
gtest_discover_tests(dagforge_tests
  DISCOVERY_MODE PRE_TEST
  PROPERTIES TIMEOUT 10
  TEST_FILTER "-PersistenceTest.*:PersistenceOpenTest.*:ApiE2EIntegrationTest.*"
)

# Persistence tests: single CTest entry so they run serially and share the
# MySQL DB without cross-test contamination.
add_test(
  NAME PersistenceTests
  COMMAND $<TARGET_FILE:dagforge_tests>
          --gtest_filter="PersistenceTest.*:PersistenceOpenTest.*"
)
set_tests_properties(PersistenceTests PROPERTIES
  TIMEOUT 60
  RESOURCE_LOCK dagforge_mysql_db
)

# API E2E tests also share runtime/db/network resources and are flaky when
# executed in parallel; run them in one serialized entry.
add_test(
  NAME ApiE2EIntegrationTests
  COMMAND $<TARGET_FILE:dagforge_tests>
          --gtest_filter="ApiE2EIntegrationTest.*"
)
set_tests_properties(ApiE2EIntegrationTests PROPERTIES
  TIMEOUT 120
  RESOURCE_LOCK dagforge_mysql_db
)

# Benchmark target
add_executable(dagforge_benchmarks
  test_utils.hpp
  bench_utils.hpp

  # Dimension 1: Core Runtime
  bench_runtime.cpp

  # Dimension 2: DAG Engine
  bench_dag_engine.cpp

  # Dimension 4: XCom & Variables
  bench_xcom.cpp

  # Dimension 5: Config parsing
  bench_config_parse.cpp

  # Dimension 6: WebSocket broadcast fanout
  bench_websocket_broadcast.cpp

  # Dimension 7: MySQL storage
  bench_mysql_storage.cpp

  # Dimension 8: Arena/memory behavior
  bench_memory_arena.cpp

)

target_link_libraries(dagforge_benchmarks
  PRIVATE
    DAGForge::lib
    benchmark::benchmark
    benchmark::benchmark_main
    Threads::Threads
)

target_include_directories(dagforge_benchmarks
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

dagforge_configure_target(dagforge_benchmarks)
