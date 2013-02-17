#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
using namespace std;

graphlab::graph_database_sharedmem* createDatabase() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));
  size_t nshards = 4;

  size_t nverts = 100;
  size_t nedges = 2000;
  boost::hash<size_t> hash; 
  // Creates a random graph
  for (size_t i = 0; i < nedges; i++) {
    size_t source = hash(i) % nverts;
    size_t target = hash(-i) % nverts;
    db.add_edge(source, target);
  }
  ASSERT_EQ(db.num_edges(), nedges);
  ASSERT_LE(db.num_vertices(),nverts);

  return (new graphlab::graph_database_sharedmem (vertexfields, edgefields, nshards));
}

void testReadVertexData(graphlab::graph_database_server* server) {
  bool success;
  graphlab::graph_database db = server->get_database();
}

void testReadField(graphlab::graph_database_server* server) {
  bool success = false;
  vector<graphlab::graph_field> vfield;
  vector<graphlab::graph_field> efield;
  std::string vfieldstr = server->get_vertex_fields();
  graphlab::iarchive iarc_vfield(vfieldstr.c_str(), vfieldstr.length());
  iarc_vfield >> success >> vfield;
  ASSERT_TRUE(success);
  ASSERT_EQ(vfield.size(), server->get_database()->get_vertex_fields().size());

  std::string efieldstr = server->get_edge_fields();
  graphlab::iarchive iarc_efield(vfieldstr.c_str(), vfieldstr.length());
  iarc_efield >> success >> efield;
  ASSERT_TRUE(success);
  ASSERT_EQ(efield.size(), server->get_database()->get_edge_fields().size());
}

int main(int argc, char** argv)
{
  graphlab::graph_database_sharedmem* db = createDatabase();
  graphlab::graph_database_server server(db);
  delete db;
  return 0;
}
