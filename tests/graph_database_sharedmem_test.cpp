#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/logger/assertions.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
using namespace std;


/**
 * Test adding vertices and change value
 */
void testAddVertex() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  // vertexfields.push_back(graphlab::graph_field("id", graphlab::VID_TYPE));
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));

  int nshards = 4;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);

  // Add 100 vertices
  size_t nverts_expected = 100;
  for (size_t i = 0; i < nverts_expected; i++) {
    db.add_vertex(i);
  }
  ASSERT_EQ(db.num_vertices(), nverts_expected);
  // Add the same vertices should not succeed
  for (size_t i = 0; i < nverts_expected; i++) {
    ASSERT_FALSE(db.add_vertex(i));
  }
  ASSERT_EQ(db.num_vertices(), nverts_expected);

  // Assign the id field of each vertex
  for (size_t i = 0; i < nverts_expected; i++) {
    graphlab::graph_vertex* v = db.get_vertex(i); 
    graphlab::graph_row* data = v->data();
    // Assert all feilds are null values
    for (size_t j = 0; j < data->num_fields(); j++) {
        ASSERT_TRUE(data->get_field(j)->is_null());
    }
    // Set pagerank field to 1.0 and url field to "http://$vid"
    ASSERT_TRUE(data->get_field("pagerank") != NULL);

    ASSERT_TRUE(data->get_field("url") != NULL);
    data->get_field("pagerank")->set_double(1.0);
    ASSERT_TRUE(data->get_field("pagerank")->get_modified());

    string url="http://" + boost::lexical_cast<string>(i);
    data->get_field("url")->set_string(url);
    ASSERT_TRUE(data->get_field("url")->get_modified());

    v->write_changes();
    ASSERT_TRUE(!data->get_field("pagerank")->get_modified());
    ASSERT_TRUE(!data->get_field("url")->get_modified());
    db.free_vertex(v);
  }

  for (size_t i = 0; i < nverts_expected; i++) {
    graphlab::graph_vertex* v = db.get_vertex(i); 
    graphlab::graph_row* data = v->data();
    // Assert all feilds are NOT null values
    for (size_t j = 0; j < data->num_fields(); j++) {
        ASSERT_FALSE(data->get_field(j)->is_null());
    }
    // Verify that pagerank field is set to 1.0 and url field to "http://$vid"
    double pr;
    ASSERT_TRUE(data->get_field("pagerank")->get_double(&pr));
    ASSERT_TRUE(fabs(pr-1) < 1e-5);
    string url;
    ASSERT_TRUE(data->get_field("url")->get_string(&url));
    ASSERT_EQ(url, "http://" + boost::lexical_cast<string>(i));  

    ASSERT_TRUE(!data->get_field("pagerank")->get_modified());
    ASSERT_TRUE(!data->get_field("url")->get_modified());
    db.free_vertex(v);
  }
}

/**
 * Test adding edges and change value
 */
void testAddEdge() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));

  size_t nshards = 4;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);

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

  // Set the weight on (i->j) to be 1/i.num_out_edges
  std::vector<double> weights;
  for (size_t i = 0 ; i < nverts; ++i) {
    graphlab::graph_vertex* v = db.get_vertex(i); 
    if (v == NULL) 
      continue;

    // Get out edges and compute the total.
    size_t num_out_edges = 0;
    std::vector<std::vector<graphlab::graph_edge*> > outadjs(db.num_shards());
    for (size_t j = 0; j < db.num_shards(); j++) {
      v->get_adj_list(j, true, NULL, &outadjs[j]);
      num_out_edges += outadjs[j].size();
    }

    // Set out edges weights.
    for (size_t j = 0; j < db.num_shards(); j++) {
      for (size_t k = 0; k < outadjs[j].size(); k++) {
        outadjs[j][k]->data()->get_field("weight")->set_double(1.0/num_out_edges);
        outadjs[j][k]->write_changes();
      }
    }

    // Free out edges pointers.
    for (size_t j = 0; j <  db.num_shards(); j++) {
      db.free_edge_vector(&outadjs[j]);
      db.free_edge_vector(&outadjs[j]);
    }
    db.free_vertex(v);
  }
}


/**
 * Test gettting shards
 */
void testShardAPI() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));
  size_t nshards = 16;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);
  size_t nverts = 100;
  size_t nedges = 2000;
  boost::hash<size_t> hash; 
  // Creates a random graph
  for (size_t i = 0; i < nedges; i++) {
    size_t source = hash(i) % nverts;
    size_t target = hash(-i) % nverts;
    db.add_edge(source, target);
  }

  // Count the number of vertices/edges in the shards
  size_t vtotal = 0;
  size_t etotal = 0;
  std::vector<graphlab::graph_shard*> shards;
  for (size_t i = 0; i < db.num_shards(); i++) {
    graphlab::graph_shard* shard = db.get_shard_copy(i); 
    shards.push_back(shard);
    vtotal += shard->num_vertices();
    etotal += shard->num_edges();
  }
  ASSERT_EQ(vtotal, db.num_vertices());
  ASSERT_EQ(etotal, db.num_edges());


  // Transform vertex by setting the url field 
  for (size_t i = 0; i < shards.size(); i++) {
    for (size_t j = 0; j < shards[i]->num_vertices(); j++) {
      shards[i]->vertex_data(j)->get_field("url")->
          set_string("http://" + boost::lexical_cast<string>(shards[i]->vertex(j)));
    }
  }

  // Transform edge by setting the weight field
  boost::unordered_map<graphlab::graph_vid_t, size_t> outedges;
  for (size_t i = 0; i < shards.size(); i++) {
    for (size_t j = 0; j < shards[i]->num_edges(); j++) {
      std::pair<graphlab::graph_vid_t, graphlab::graph_vid_t> pair = shards[i]->edge(j);
      outedges[pair.first]++;
    }
  }
  for (size_t i = 0; i < shards.size(); i++) {
    for (size_t j = 0; j < shards[i]->num_edges(); j++) {
      std::pair<graphlab::graph_vid_t, graphlab::graph_vid_t> pair = shards[i]->edge(j);
      graphlab::graph_value* val = shards[i]->edge_data(j)->get_field("weight");
      ASSERT_TRUE(val != NULL);
      ASSERT_TRUE(val->is_null());
      val->set_double(1.0/outedges[pair.first]);
    }
  }

  // Commit all changes;
  for (size_t i = 0; i < shards.size(); i++) {
    db.commit_shard(shards[i]);
    db.free_shard(shards[i]);
  }
}

int main(int argc, char** argv) {
  testAddVertex();
  testAddEdge();
  testShardAPI();
  return 0;
}
