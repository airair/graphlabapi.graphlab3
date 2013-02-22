#include <graphlab/database/distributed_graph/distributed_graph.hpp>
#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include "graph_database_test_util.hpp"

#include <vector>
using namespace std;


void test(graphlab::graph_database* db) {
  graphlab::graph_database_server server(db);
  graphlab::distributed_graph graph(&server);
  ASSERT_EQ(graph.num_shards(), 0);
  // ASSERT_EQ(graph.get_vertex_fields().size(), db->get_vertex_fields(),size());
  // ASSERT_EQ(graph.get_edge_fields().size(), db->get_edge_fields().size());
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
  graphlab::graph_database* db = graphlab::graph_database_test_util::createDatabase(nverts, nedges, nshards, vertexfields, edgefields);
  test(db);
  return 0;
}
