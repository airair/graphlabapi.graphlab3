#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/graph_sharding_constraint.hpp>
#include <graphlab/database/sharedmem_database/graph_vertex_index.hpp>
#include <graphlab/database/sharedmem_database/graph_edge_index.hpp>
#include <graphlab/database/sharedmem_database/graph_vertex_sharedmem.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
/**
 * \ingroup group_graph_database_sharedmem
 * An shared memory implementation of a graph database
 */
class graph_database_sharedmem : public graph_database {
  // schema for vertex and edge datatypes
  std::vector<graph_field> vertex_fields;
  std::vector<graph_field> edge_fields;

  // simulates backend storage of data
  std::vector<graph_shard> shards;
  // Array stores the vertex data.
  std::vector<graph_row*> vertex_store; 
  // dependencies between shards
  sharding_constraint sharding_graph;

  // index service for fine grained queries 
  graph_vertex_index vertex_index;
  std::vector<graph_edge_index> edge_index;

  size_t _num_edges;

 public:
   graph_database_sharedmem(std::vector<graph_field> vertex_fields,
                            std::vector<graph_field> edge_fields,
                            size_t numshards) :sharding_graph(numshards, "grid") { 
     _num_edges = 0; 
     shards.resize(numshards);
     edge_index.resize(numshards);
   }

   virtual ~graph_database_sharedmem() {
     for (size_t i = 0; i < shards.size(); ++i) {
       shards[i].clear();
     }
     for (size_t i = 0; i < vertex_store.size(); i++) {
       vertex_store[i]->_data.clear();
     }
   }

  /**
   * Returns the number of vertices in the graph.
   * This may be slow.
   */
  uint64_t num_vertices() {
    return vertex_store.size();
  };
  
  /**
   * Returns the number of edges in the graph.
   * This may be slow.
   */
  uint64_t num_edges() {
    return _num_edges;
  }

  /**
   * Returns the field metadata for the vertices in the graph
   */
  const std::vector<graph_field>& get_vertex_fields() {
    return vertex_fields;
  };

  /**
   * Returns the field metadata for the edges in the graph
   */
  const std::vector<graph_field>& get_edge_fields() {
    return edge_fields;
  };


  // -------- Fine grained API ------------

  /** returns a vertex in ret_vertex for a queried vid. Returns NULL on failure
   * The returned vertex pointer must be freed using free_vertex
   */
  graph_vertex* get_vertex(graph_vid_t vid) {
    size_t idx = vertex_index.get_index(vid);
    return (new graph_vertex_sharedmem(vid, vertex_store[idx], &edge_index, this));
  };

  /**
   *  Finds a vertex using an indexed integer field. Returns the vertex IDs
   *  in out_vids corresponding to the vertices where the integer field 
   *  identified by fieldpos has the specified value.
   *  Return true on success, and false on failure.
   */
  bool find_vertex(size_t fieldpos,
                           graph_int_t value, 
                           std::vector<graph_vid_t>* out_vids) {
    // not implemented
    ASSERT_TRUE(false);
    return false;
  };

  /**
   *  Finds a vertex using an indexed string field. Returns the vertex IDs
   *  in out_vids corresponding to the vertices where the string field 
   *  identified  by fieldpos has the specified value.
   *  Return true on success, and false on failure.
   */
  bool find_vertex(size_t fieldpos,
                           graph_string_t value, 
                           std::vector<graph_vid_t>* out_vids) {
    // not implemented
    ASSERT_TRUE(false);
    return false;
  };

  /**
   * Frees a vertex object
   */
  void free_vertex(graph_vertex* vertex) {
    delete vertex;
    vertex = NULL;
  };

  /**
   * Frees a single edge object
   */
  void free_edge(graph_edge* edge) {
    delete edge;
    edge = NULL;
  }
  
  /**
   * Frees a collection of edges. The vector will be cleared. on return.
   */
  void free_edge_vector(std::vector<graph_edge*>* edgelist) {
    foreach(graph_edge*& e, *edgelist)
        free_edge(e);
    edgelist->clear();
  }


//  ------ Coarse Grained API ---------

  /**
   * Returns the number of shards in the database
   */
  size_t num_shards() { return shards.size(); }
  
  
  /**
   * Synchronously obtains a shard from the database.
   * Returns NULL on failure
   */
  graph_shard* get_shard(graph_shard_id_t shard_id) {
    return &shards[shard_id];
  }
                          
  /**
   * Gets the contents of the shard which are adjacent to some other shard
   * Returns NULL on failure
   */
  graph_shard* get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                                 graph_shard_id_t adjacent_to) {
    // not implemented
    ASSERT_TRUE(false);
    return NULL;
  }
  /**
   * Frees a shard.
   */  
  void free_shard(graph_shard* shard) {
    shard->clear();
  }
  
  /** 
   * Returns a list of shards IDs which adjacent to a given shard id
   */
  void adjacent_shards(graph_shard_id_t shard_id, 
                               std::vector<graph_shard_id_t>* out_adj_shard_ids) { 
    sharding_graph.get_neighbors(shard_id, *out_adj_shard_ids);
  }

  /**
   * Commits all the changes made to the vertex data and edge data 
   * in the shard, resetting all modification flags.
   */
  void commit_shard(graph_shard* shard) {
    // not implemented 
    ASSERT_TRUE(false);
  }

// ----------- Modification API -----------------
  /*
   * Insert the vertex v into a shard = hash(v) as master
   * Return false if v is already inserted.
   */
  bool add_vertex(graph_vid_t vid) {
    if (vertex_index.has_vertex(vid)) {
      return false;
    }
    // create a new row of all null values.
    graph_row* row = new graph_row(this, vertex_fields);
    row->_is_vertex = true;
    vertex_store.push_back(row);
    // update vertex index 
    vertex_index.add_vertex(vid, row, vertex_store.size()-1);
    return true;
  }

  /**
   * Insert an edge from source to target with empty value.
   * Also insert vertex copies into the corresponding shards.
   */
  void add_edge(graph_vid_t source, graph_vid_t target) {
    boost::hash<std::pair<graph_vid_t, graph_vid_t> > edge_hash;
    graph_shard_id_t shardid = edge_hash(std::pair<graph_vid_t, graph_vid_t>(source, target)) % shards.size();

    // create a new row of all null values
    graph_row* row = new graph_row(this, edge_fields);
    row->_is_vertex = false;
    size_t pos = get_shard(shardid)->add_edge(source, target, row);
    _num_edges++;

    // update_edge_index
    edge_index[shardid].add_edge(source, target, pos);

    // add source vertex to the shard if it was not there before
    if (!vertex_index.has_vertex(source)) {
      add_vertex(source);
    }

    // add target vertex to the shard  if it was not there before
    if (!vertex_index.has_vertex(target)) {
      add_vertex(target);
    }
  }
};
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
