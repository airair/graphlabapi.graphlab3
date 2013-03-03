#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/logger/assertions.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
using namespace std;

/**
 * Test adding vertices, change value and commit
 */
void testAddVertex() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));

  int nshards = 4;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);
  const graphlab::sharding_constraint& constraint_graph = db.get_sharding_constraint();

  // Add 100 vertices
  size_t nverts_expected = 100000;

  std::cout << "Test adding vertices. Num vertices = " << nverts_expected << std::endl;

  for (size_t i = 0; i < nverts_expected; i++) {
    graphlab::graph_shard_id_t master = constraint_graph.get_master(i);
    db.add_vertex(i, master);
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
    ASSERT_TRUE(data->get_field(0) != NULL);

    ASSERT_TRUE(data->get_field(1) != NULL);
    data->get_field(0)->set_double(1.0);
    ASSERT_TRUE(data->get_field(0)->get_modified());

    string url="http://" + boost::lexical_cast<string>(i);
    data->get_field(1)->set_string(url);
    ASSERT_TRUE(data->get_field(1)->get_modified());

    v->write_changes();
    ASSERT_TRUE(!data->get_field(0)->get_modified());
    ASSERT_TRUE(!data->get_field(1)->get_modified());
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
    ASSERT_TRUE(data->get_field(0)->get_double(&pr));
    ASSERT_TRUE(fabs(pr-1) < 1e-5);
    string url;
    ASSERT_TRUE(data->get_field(1)->get_string(&url));
    ASSERT_EQ(url, "http://" + boost::lexical_cast<string>(i));  

    ASSERT_TRUE(!data->get_field(0)->get_modified());
    ASSERT_TRUE(!data->get_field(1)->get_modified());
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
  const graphlab::sharding_constraint& constraint_graph = db.get_sharding_constraint();

  size_t nverts = 10000; // 10k vertices
  size_t nedges = 500000; // 500k edges

  std::cout << "Test add edges. Num edges =  " << nedges << std::endl;

  for (size_t i = 0; i < nverts; i++) {
    graphlab::graph_shard_id_t master = constraint_graph.get_master(i);
    db.add_vertex(i, master);
  }

  boost::hash<size_t> hash; 

  // Creates a random graph
  for (size_t i = 0; i < nedges; i++) {
    size_t source = hash(i) % nverts;
    size_t target = hash(-i) % nverts;

    graphlab::graph_shard_id_t master = constraint_graph.get_master(source, target);
    db.add_edge(source, target, master);
    db.add_vertex_mirror(source, constraint_graph.get_master(source), master);
    db.add_vertex_mirror(target, constraint_graph.get_master(target), master);
  }

  ASSERT_EQ(db.num_edges(), nedges);
  ASSERT_LE(db.num_vertices(),nverts);

  std::cout << "Test transform edges." << std::endl;

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
        outadjs[j][k]->data()->get_field(0)->set_double(1.0/num_out_edges);
        outadjs[j][k]->write_changes();
      }
    }

    // Free out edges pointers.
    for (size_t j = 0; j <  db.num_shards(); j++) {
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
  edgefields.push_back(graphlab::graph_field("dummy", graphlab::INT_TYPE));
  size_t nshards = 16;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);
  size_t nverts = 6400;
  size_t nedges = 128000;
  boost::hash<size_t> hash; 

  const graphlab::sharding_constraint& constraint_graph = db.get_sharding_constraint();


  for (size_t i = 0; i < nverts; i++) {
    db.add_vertex(i, constraint_graph.get_master(i));
  }

  std::cout << "Test shard api" << std::endl;
  std::cout << "Creating graph: nverts = " << nverts << " nedges = " << nedges
            << "nshards = " << nshards << std::endl;

  // Creates a random graph
  for (size_t i = 0; i < nedges; i++) {
    size_t source = hash(i) % nverts;
    size_t target = hash(-i) % nverts;
    graphlab::graph_shard_id_t master = constraint_graph.get_master(source, target);
    db.add_edge(source, target, master);
    db.add_vertex_mirror(source, constraint_graph.get_master(source), master);
    db.add_vertex_mirror(target, constraint_graph.get_master(target), master);
  }

  // Count the number of vertices/edges in the shards
  size_t vtotal = 0;
  size_t etotal = 0;
  std::vector<graphlab::graph_shard*> shards;
  for (size_t i = 0; i < db.num_shards(); i++) {
    graphlab::graph_shard* shard = db.get_shard_copy(i); 
    for (size_t j = 0; j < shard->num_vertices();j++) {
      ASSERT_TRUE(shard->vertex_data(j)->is_vertex());
      ASSERT_EQ(shard->vertex_data(j)->num_fields(), 1);
    }
    for (size_t j = 0; j < shard->num_edges();j++) {
      ASSERT_TRUE(!shard->edge_data(j)->is_vertex());
      ASSERT_EQ(shard->edge_data(j)->num_fields(),2);
    }
    vtotal += shard->num_vertices();
    etotal += shard->num_edges();
    shards.push_back(shard);
  }
  ASSERT_EQ(vtotal, db.num_vertices());
  ASSERT_EQ(etotal, db.num_edges());


  std::cout << "Transform vertices ... " << std::endl;
  // Transform vertex by setting the url field 
  for (size_t i = 0; i < shards.size(); i++) {
    for (size_t j = 0; j < shards[i]->num_vertices(); j++) {
      graphlab::graph_row* row = shards[i]->vertex_data(j);
      ASSERT_TRUE(row->is_vertex());
      row->get_field(0)->set_string("http://" + boost::lexical_cast<string>(shards[i]->vertex(j)));
    }
  }

  std::cout << "Transform edges... " << std::endl;
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
      graphlab::graph_value* val = shards[i]->edge_data(j)->get_field(0);
      ASSERT_TRUE(val != NULL);
      ASSERT_TRUE(val->is_null());
      val->set_double(1.0/outedges[pair.first]);
      ASSERT_TRUE(shards[i]->edge_data(j)->get_field(1)->set_integer(0));
    }
  }

  std::cout << "Commit changes... " << std::endl;
  // Commit all changes;
  for (size_t i = 0; i < shards.size(); i++) {
    db.commit_shard(shards[i]);
    db.free_shard(shards[i]);
  }

  std::cout << "Verify changes... " << std::endl;
  // Get content of shard i that is adjacent to shard j
  // Set every edge in the intersection to 0.0.
  // This should traverse every edge twice.
  for (size_t i = 0; i < db.num_shards(); i++) {
    for (size_t j = 0; j < db.num_shards(); j++) {
      graphlab::graph_shard* shardij = db.get_shard_contents_adj_to(i, j);
      for (size_t k = 0; k < shardij->num_edges(); k++) {
        graphlab::graph_value* val = shardij->edge_data(k)->get_field(1);
        ASSERT_TRUE(val != NULL);
        ASSERT_TRUE(!val->is_null());
        graphlab::graph_int_t oldval;
        ASSERT_TRUE(val->get_integer(&oldval));
        val->set_integer(oldval+1);
      }
    db.commit_shard(shardij);
    db.free_shard(shardij);
    }
  }

  // Check "dummy" in every edge is set to 2 now. 
  for (size_t i = 0; i < db.num_shards(); i++) {
    graphlab::graph_shard* shard = db.get_shard(i);
    for (size_t j = 0; j < shard->num_edges(); j++) {
      graphlab::graph_int_t dummyval;
      ASSERT_TRUE(shard->edge_data(j)->get_field(1)->get_integer(&dummyval));
      if (constraint_graph.get_master(shard->edge(j).first) == constraint_graph.get_master(shard->edge(j).second)) {
        ASSERT_EQ(dummyval, 1);
      } else {
        ASSERT_EQ(dummyval, 2);
      }
    }
  }
}

int main(int argc, char** argv) {
  testAddVertex();
  testAddEdge();
  testShardAPI();
  return 0;
}
