#include <graphlab/database/server/graph_shard_server.hpp>
#include <graphlab/database/errno.hpp>
#include "graph_database_test_util.hpp"
using namespace std;
typedef graphlab::graph_database_test_util testutil;
/**
 * Test add fields
 */
void testFieldAPI() {
  cout << "Test vertex/edge fields...." << endl;
  size_t nverts = 10;
  size_t nedges = 20;
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));
  edgefields.push_back(graphlab::graph_field("dummy", graphlab::STRING_TYPE));

  graphlab::graph_shard_server& server = 
      *(testutil::createShardServer(nverts, nedges, 0,
                                   vector<graphlab::graph_field>(),
                                   vector<graphlab::graph_field>()));

  // adding fields
  for (size_t i = 0; i < vertexfields.size(); i++) {
    server.add_vertex_field(vertexfields[i]);
  }
  for (size_t i = 0; i < edgefields.size(); i++) {
    server.add_edge_field(edgefields[i]);
  }

  for (size_t i = 0; i < vertexfields.size(); i++) {
    ASSERT_EQ(server.add_vertex_field(vertexfields[i]), EDUP);
  }
  for (size_t i = 0; i < edgefields.size(); i++) {
    ASSERT_EQ(server.add_edge_field(edgefields[i]), EDUP);
  }

  // test compare fields
  const vector<graphlab::graph_field>& actual_vertexfields = server.get_vertex_fields();
  const vector<graphlab::graph_field>& actual_edgefields = server.get_edge_fields();

  for (size_t i = 0; i < vertexfields.size(); i++) {
    ASSERT_TRUE(testutil::compare_graph_field(vertexfields[i], actual_vertexfields[i]));
  }
  for (size_t i = 0; i < edgefields.size(); i++) {
    ASSERT_TRUE(testutil::compare_graph_field(edgefields[i], actual_edgefields[i]));
  }

  graphlab::graph_shard& shard = server.get_shard();
    for (size_t j = 0; j < shard.num_vertices(); j++) {
      graphlab::graph_row& row = *shard.vertex_data(j);
      ASSERT_TRUE(row.is_null());
      ASSERT_TRUE(row.is_vertex());
      ASSERT_EQ(row.num_fields(), vertexfields.size());
      for (size_t k = 0; k < row.num_fields(); k++) {
        ASSERT_EQ(row.get_field(k)->type(), vertexfields[k].type);
      }
    }

    for (size_t j = 0; j < shard.num_edges(); j++) {
      graphlab::graph_row& row = *shard.edge_data(j);
      ASSERT_TRUE(row.is_null());
      ASSERT_TRUE(!row.is_vertex());
      ASSERT_EQ(row.num_fields(), edgefields.size());
      for (size_t k = 0; k < row.num_fields(); k++) {
        ASSERT_EQ(row.get_field(k)->type(), edgefields[k].type);
      }
    }
  delete &server;
}

/**
 * Test adding vertices, change value and commit
 */
void testVertexAPI() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));

  graphlab::graph_shard_server server(0, vertexfields, edgefields);

  // Add some vertices
  size_t nverts_expected = 100000;
  graphlab::graph_row empty_vdata(vertexfields, true);
  cout << "Test adding vertices. Num vertices = " << nverts_expected << endl;
  for (size_t i = 0; i < nverts_expected; i++) {
    server.add_vertex(i, empty_vdata);
  }
  ASSERT_EQ(server.num_vertices(), nverts_expected);

  // for (size_t i = 0; i < nverts_expected; i++) {
  //   // Assert all feilds are null values
  //   ASSERT_TRUE(v.data.is_null());
  //   for (size_t j = 0; j < v.data.num_fields(); j++) {
  //       ASSERT_TRUE(v.data.get_field(j)->is_null());
  //   }
  // }
  // for (size_t i = 0; i < 
  //   // Set pagerank field to 1.0 and url field to "http://$vid"
  //   graphlab::graph_row new_data(vertexfields, true);
  //   new_data.get_field(0)->set_double(1.0);
  //   string url="http://" + boost::lexical_cast<string>(i);
  //   new_data.get_field(1)->set_string(url);
  //   vnew.vid = i;
  //   vnew.data = new_data;
  //   changelist.push_back(vnew);



  // for (size_t i = 0; i < nverts_expected; i++) {
  //   vertex_descriptor v = server.get_vertex(i);
  //   graphlab::graph_row& data = v.data;
  //   ASSERT_FALSE(data.is_null());
  //   // Assert all feilds are NOT null values
  //   for (size_t j = 0; j < v.data.num_fields(); j++) {
  //       ASSERT_FALSE(data.get_field(j)->is_null());
  //   }
  //   // Verify that pagerank field is set to 1.0 and url field to "http://$vid"
  //   double pr;
  //   ASSERT_TRUE(data.get_field(0)->get_double(&pr));
  //   ASSERT_TRUE(fabs(pr-1) < 1e-5);
  //   string url;
  //   ASSERT_TRUE(data.get_field(1)->get_string(&url));
  //   ASSERT_EQ(url, "http://" + boost::lexical_cast<string>(i));  
  // }
}

/**
 * Test adding edges and change value
 */
void testEdgeAPI() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));

  graphlab::graph_shard_server server(0, vertexfields, edgefields);

  size_t nverts = 10000; // 10k vertices
  size_t nedges = 500000; // 500k edges
  cout << "Test add edges. Num edges =  " << nedges << endl;

  graphlab::graph_row empty_vdata(vertexfields, true);
  graphlab::graph_row empty_edata(edgefields, false);
  for (size_t i = 0; i < nverts; i++) {
    int err = server.add_vertex(i, empty_vdata);
    ASSERT_EQ(err, 0);
  }
  boost::hash<size_t> hash; 

  // // Creates a random graph
  for (size_t i = 0; i < nedges; i++) {
    size_t source = hash(i) % nverts;
    size_t target = hash(-i) % nverts;
    int err = server.add_edge(source, target, empty_edata);
    ASSERT_EQ(err, 0);
  }
  ASSERT_EQ(server.num_edges(), nedges);
  ASSERT_LE(server.num_vertices(),nverts);

  // cout << "Test transform edges." << endl;
  // // Set the weight on (i.j) to be 1/i.num_out_edges
  // vector<double> weights;
  // for (size_t i = 0 ; i < nverts; ++i) {
  //   size_t master = testutil::get_master(i, nshards);
  //   graphlab::graph_vertex* v = db.get_vertex(i, master); 
  //   if (v == NULL) 
  //     continue;

  //   // Get out edges and compute the total.
  //   size_t num_out_edges = 0;
  //   vector<vector<graphlab::graph_edge*> > outadjs(db.num_shards());
  //   for (size_t j = 0; j < db.num_shards(); j++) {
  //     v.get_adj_list(j, true, NULL, &outadjs[j]);
  //     num_out_edges += outadjs[j].size();
  //   }

  //   // Set out edges weights.
  //   for (size_t j = 0; j < db.num_shards(); j++) {
  //     for (size_t k = 0; k < outadjs[j].size(); k++) {
  //       outadjs[j][k].get_field(0)->set_double(1.0/num_out_edges);
  //       outadjs[j][k].write_changes();
  //     }
  //   }
  //   // Free out edges pointers.
  //   for (size_t j = 0; j <  db.num_shards(); j++) {
  //     db.free_edge_vector(outadjs[j]);
  //   }
  //   db.free_vertex(v);
  // }
}


int main(int argc, char** argv) {
  testFieldAPI();
  testVertexAPI();
  testEdgeAPI();
  return 0;
}
