#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/logger/assertions.hpp>
#include <vector>
using namespace std;


void testAddVertex() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("id", graphlab::VID_TYPE));
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  int nshards = 4;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);

  db.add_vertex(0);
  graphlab::graph_vertex* v = db.get_vertex(0);
  graphlab::graph_row* data = v->data();
  ASSERT_EQ(db.num_vertices(), 1);
  ASSERT_TRUE(data->is_vertex());

  // return false when adding the same vertex.
  ASSERT_FALSE(db.add_vertex(0));
  ASSERT_TRUE(db.add_vertex(1));
  ASSERT_EQ(db.num_vertices(), 2);
}


void testAddEdge() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("id", graphlab::VID_TYPE));
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  size_t nshards = 4;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);

  db.add_edge(1,2);
  db.add_edge(1,3);
  db.add_edge(1,4);
  db.add_edge(3,4);
  db.add_edge(4,1);
  ASSERT_EQ(db.num_edges(),5);
  ASSERT_EQ(db.num_vertices(),4);

  graphlab::graph_vertex* v = db.get_vertex(1);
  std::vector<graphlab::graph_edge*> inedges;
  std::vector<graphlab::graph_edge*> outedges;
  size_t totalin = 0;
  size_t totalout = 0;
  for (size_t i = 0 ; i < nshards; ++i) {
    v->get_adj_list(i, true, &inedges, &outedges);
    totalin += inedges.size();
    totalout += outedges.size();
    db.free_edge_vector(&inedges);
    db.free_edge_vector(&outedges);
  }
  ASSERT_EQ(totalin, 1); 
  ASSERT_EQ(totalout, 3); 

}

int main(int argc, char** argv) {
  testAddVertex();
  testAddEdge();
  return 0;
}
