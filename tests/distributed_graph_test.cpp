#include <graphlab/database/distributed_graph/distributed_graph.hpp>
#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include "graph_database_test_util.hpp"

#include <vector>
using namespace std;

typedef graphlab::graph_database_test_util test_util;

void test(graphlab::graph_database* db) {
  graphlab::graph_database_server server(db);
  graphlab::distributed_graph graph(&server);


  /* Test intialize graph */
  ASSERT_EQ(graph.num_shards(), db->num_shards());
  for (size_t i = 0; i < graph.get_vertex_fields().size(); i++) {
     ASSERT_TRUE(test_util::compare_graph_field(graph.get_vertex_fields()[i], db->get_vertex_fields()[i]));
  }
  for (size_t i = 0; i < graph.get_edge_fields().size(); i++) {
     ASSERT_TRUE(test_util::compare_graph_field(graph.get_edge_fields()[i], db->get_edge_fields()[i]));
  }

  /* Test get shard equality */
  for (size_t i = 0; i < graph.num_shards(); ++i) {
    graphlab::graph_shard* shard = graph.get_shard(i);
    ASSERT_TRUE(test_util::compare_shard((*shard), *(db->get_shard(i))));
    graph.free_shard(shard);
  } 
}



int main(int argc, char** argv) {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));
  size_t nshards = 4;
  size_t nverts = 100;
  size_t nedges = 100;
  graphlab::graph_database* db =
      test_util::createDatabase(nverts, nedges, nshards, vertexfields, edgefields);
  test(db);
  return 0;
}
