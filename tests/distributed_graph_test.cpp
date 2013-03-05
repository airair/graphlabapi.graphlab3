#include <graphlab/database/graph_client/distributed_graph_client.hpp>
#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/graph_server/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include "graph_database_test_util.hpp"

#include <vector>
using namespace std;

typedef graphlab::graph_database_test_util test_util;

void test_shard_retrieval() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));

  size_t nshards = 4;
  size_t nverts = 100;
  size_t nedges = 100;
  graphlab::graph_database* db =
      test_util::createDatabase(nverts, nedges, nshards, vertexfields, edgefields);

  graphlab::graph_database_server server(db);
  graphlab::distributed_graph graph(&server);

  /* Test intialize graph */
  ASSERT_EQ(graph.num_shards(), db->num_shards());
  for (size_t i = 0; i < graph.get_vertex_fields().size(); i++) {
     ASSERT_TRUE(test_util::compare_graph_field(graph.get_vertex_fields()[i], vertexfields[i]));
  }
  for (size_t i = 0; i < graph.get_edge_fields().size(); i++) {
     ASSERT_TRUE(test_util::compare_graph_field(graph.get_edge_fields()[i], edgefields[i]));
  }

  /* Test get shard equality */
  for (size_t i = 0; i < graph.num_shards(); ++i) {
    graphlab::graph_shard* shard = graph.get_shard(i);
    ASSERT_TRUE(shard != NULL);
    ASSERT_TRUE(test_util::compare_shard((*shard), *(db->get_shard(i))));
    graph.free_shard(shard);
  } 
  delete(db);
}

void test_ingress(size_t nverts, size_t nedges, bool batch) {
  std::cout << "Test ingress: num_vertices = " << nverts 
            << " num_edges = " << nedges 
            << " use batch = " << batch << std::endl;

  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));

  size_t nshards = 4;
  graphlab::graph_database* db =
      test_util::createDatabase(0, 0, nshards, vertexfields, edgefields);

  graphlab::graph_database_server server(db);
  graphlab::distributed_graph graph(&server);

  std::cout << "Test adding vertices ... " << std::endl;
  for (size_t i = 0; i < nverts; i++) {
    graphlab::graph_row* row = new graphlab::graph_row(vertexfields, true);
    row->get_field(0)->set_double((double)i);
    row->get_field(0)->post_commit_state();
    if (batch)
      graph.add_vertex(i, row);
    else
      graph.add_vertex_now(i, row);
  }

  std::cout << "Test adding edges ... " << std::endl;
  // Creates a random graph
  boost::hash<graphlab::graph_vid_t> hash;
  for (size_t i = 0; i < nedges; i++) {
    size_t source = hash(i) % nverts;
    size_t target = hash(-i) % nverts;
    if (batch) 
      graph.add_edge(source, target);
    else
      graph.add_edge_now(source, target);
  }

  if (batch) {
    graph.flush();
  }

  ASSERT_EQ(graph.num_vertices(), nverts);
  ASSERT_EQ(graph.num_edges(), nedges);
  delete(db);
}

int main(int argc, char** argv) {
  test_shard_retrieval();
  test_ingress(1000, 50000, false);
  test_ingress(100000, 500000, true);
  return 0;
}
