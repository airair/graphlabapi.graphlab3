#include <graphlab/database/client/graphdb_client.hpp>
#include <graphlab/database/client/ingress/graph_loader.hpp>
#include <graphlab/database/graphdb_config.hpp>
#include <graphlab/database/util/graphdb_util.hpp>
#include <graphlab/logger/logger.hpp>

using namespace std;

void make_ring_graph(size_t nverts,
                     graphlab::graphdb_client& client) {

  typedef graphlab::graph_database::edge_insert_descriptor edge_insert_descriptor;
  typedef graphlab::graph_database::vertex_insert_descriptor vertex_insert_descriptor;
  vector<edge_insert_descriptor> edge_insertions;
  graphlab::graph_row empty_edata;
  empty_edata._is_vertex = false;
  for (size_t i = 0; i < nverts; ++i) {
    edge_insert_descriptor e;
    e.data = empty_edata;
    e.src = i;
    e.dest = (i+1) % nverts;
    edge_insertions.push_back(e);
    e.src = (i+1) % nverts;
    e.dest = i;
    edge_insertions.push_back(e);
  }
  vector<int> errorcodes;
  ASSERT_TRUE(client.add_edges(edge_insertions, errorcodes));
}

int main(int argc, char** argv) {
  if (argc != 2) {
    cout << "Usage: graphdb_test_client [config]\n";
    return 0;
  }

  string configfile = argv[1];
  graphlab::graphdb_config config(configfile);
  graphlab::graphdb_client client(config);

  // graphlab::graph_loader loader(&client);
  // loader.load_from_posixfs("/Users/haijieg/data/google_graph/web-Google.txt", "snap");
  // loader.load_from_posixfs("/Users/haijieg/data/google_graph/web-Google-tiny.txt", "snap");
  
  size_t expected_nverts = 1000; 
  size_t expected_nedges = 2*expected_nverts; 
  make_ring_graph(expected_nverts, client);

  size_t num_vertices = client.num_vertices();
  size_t num_edges = client.num_edges();

  cout << "Num vertices: " << num_vertices << endl;
  cout << "Num edges: " << num_edges << endl;


  ASSERT_EQ(expected_nverts, num_vertices);
  ASSERT_EQ(expected_nedges, num_edges);


  typedef graphlab::graph_database::vertex_adj_descriptor vertex_adj_descriptor;
  cout << "Add fields..." << endl; 
  // add vertex and edge field
  graphlab::graph_field vfield("title", graphlab::STRING_TYPE);
  graphlab::graph_field efield("weights", graphlab::DOUBLE_TYPE);
  client.add_vertex_field(vfield);
  client.add_edge_field(efield);

  cout << "Get/set vertices ..." << endl; 
  // test get/set vertex
  for (size_t i = 0; i < num_vertices; i++) {
    graphlab::graph_row out;
    if (client.get_vertex(i, out) == 0) {
      ASSERT_EQ(out.num_fields(), 1);
      ASSERT_TRUE(out.is_vertex());
      ASSERT_TRUE(out.is_null());

      string expected = "vertex"+boost::lexical_cast<string>(i);
      out.get_field(0)->set_string(expected);
      client.set_vertex(i, out);

      graphlab::graph_row out2;
      client.get_vertex(i, out2);
      ASSERT_EQ(out2.num_fields(), 1);
      ASSERT_TRUE(out2.is_vertex());
      ASSERT_FALSE(out2.is_null());
      string actual; 
      out2.get_field(0)->get_string(&actual);
      ASSERT_EQ(actual, expected);
    }
  }

  cout << "Get/set vertex adjacencies... " << endl;
  double expected = 0.0;
  for (size_t i = 0; i < num_vertices; i++) {
    vertex_adj_descriptor in_edges;
    client.get_vertex_adj(i, true, in_edges); 
    ASSERT_EQ(in_edges.size(), 2);

    vector<int> errorcodes;
    vector<graphlab::graph_row> out;
    ASSERT_TRUE(client.get_edges(in_edges.eids, out, errorcodes));

    for (size_t j = 0; j < out.size(); j++) {
      ASSERT_EQ(out[j].num_fields(), 1);
      ASSERT_TRUE(out[j].is_edge());
      ASSERT_TRUE(out[j].is_null());
    }

    for (size_t j = 0; j < out.size(); j++) {
      ASSERT_TRUE(out[j].get_field(0)->set_double(expected));
    }
    ASSERT_TRUE(client.set_edges(zip(in_edges.eids, out), errorcodes));

    errorcodes.clear();
    if ((i+1) % 10000 == 0) {
      cout << "processed " << (i+1) << " vertices" << endl;
    }
  }

  double actual = -1; // a random number
  for (size_t i = 0; i < num_vertices; i++) {
    vertex_adj_descriptor out_edges;
    client.get_vertex_adj(i, false, out_edges); 
    ASSERT_EQ(out_edges.size(), 2);

    vector<int> errorcodes;
    vector<graphlab::graph_row> out;
    ASSERT_TRUE(client.get_edges(out_edges.eids, out, errorcodes));
    for (size_t j = 0; j < out.size(); j++) {
      ASSERT_EQ(out[j].num_fields(), 1);
      ASSERT_TRUE(out[j].is_edge());
      ASSERT_FALSE(out[j].is_null());
      ASSERT_TRUE(out[j].get_field(0)->get_double(&actual));
      ASSERT_EQ(actual, expected);
    }
  }
  cout << "done" << endl;
  return 0;
}
