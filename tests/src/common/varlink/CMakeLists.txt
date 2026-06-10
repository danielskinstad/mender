add_executable(varlink_server_test EXCLUDE_FROM_ALL varlink_server_test.cpp)
target_link_libraries(varlink_server_test PUBLIC
  common_varlink
  common_testing
  main_test
  gmock
)
gtest_discover_tests(varlink_server_test NO_PRETTY_VALUES)
add_dependencies(tests varlink_server_test)
