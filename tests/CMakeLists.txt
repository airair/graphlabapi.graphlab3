project(tests)
add_graphlab_executable(graph_database_test graph_database_test.cpp ../../src/graphlab/database/kvstore_mysql.cpp ../../src/graphlab/database/kvstore_mongodb.cpp)
set(MACPORTS_INCLUDE /opt/local/include)
set(MYSQL_INCLUDE ${MACPORTS_INCLUDE}/mysql5/mysql)
set(MYSQL_NDB ${MYSQL_INCLUDE}/storage/ndb)
include_directories(${MYSQL_INCLUDE} ${MYSQL_NDB} ${MYSQL_NDB}/ndbapi)
target_link_libraries(graph_database_test mysqlclient_r mysqld dl ndbclient z m ssl crypto mystrings)

add_graphlab_executable(comm_test comm_test.cpp)

add_graphlab_executable(comm_bench comm_bench.cpp)

add_graphlab_executable(qthread_basic_test qthread_basic_test.cpp)

add_graphlab_executable(graph_shard_server_test graph_shard_server_test.cpp)

add_graphlab_executable(graphdb_test_server graphdb_test_server.cpp)

#add_graphlab_executable(graph_database_sharedmem_test  graph_database_sharedmem_test.cpp)


#add_graphlab_executable(graph_database_test_server graph_database_test_server.cpp)

#add_graphlab_executable(pagerank_test_server pagerank_test_server.cpp)

#add_graphlab_executable(graph_database_test_client graph_database_test_client.cpp)

#add_graphlab_executable(distributed_graph_test distributed_graph_test.cpp)

add_graphlab_executable(qthread_sgd qthread_sgd.cpp)

#add_graphlab_executable(generate_pds generate_pds.cpp)
