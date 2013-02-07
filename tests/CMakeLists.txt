project(tests)
add_graphlab_executable(zookeeper_test zookeeper_test.cpp)

add_graphlab_executable(graph_database_test graph_database_test.cpp)
##target_link_libraries(graph_database_test mongoclient boost_thread-mt boost_filesystem-mt boost_system-mt)
##target_link_libraries(graph_database_test mongoclient boost_thread-mt boost_filesystem-mt boost_program_options-mt boost_system-mt)

add_graphlab_executable(comm_test comm_test.cpp)

add_graphlab_executable(comm_bench comm_bench.cpp)

add_graphlab_executable(qthread_basic_test qthread_basic_test.cpp)

add_graphlab_executable(graph_database_sharedmem_test  graph_database_sharedmem_test.cpp)

add_graphlab_executable(qthread_sgd qthread_sgd.cpp)

add_graphlab_executable(generate_pds generate_pds.cpp)
