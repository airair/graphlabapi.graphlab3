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

  // index service for vertex-shard query 
  boost::unordered_map<graph_vid_t, graph_shard_id_t> vid2master;
  boost::unordered_map<graph_shard_id_t, std::vector<graph_vid_t> > master2vid;
  boost::unordered_map<graph_vid_t, boost::unordered_set<graph_shard_id_t> > vid2mirrors;

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
    return (new graph_vertex_sharedmem(vid, vertex_store[idx], vid2master[vid], vid2mirrors[vid], &edge_index, this));
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
    return new graph_shard(shards[shard_id]);
  }
                          
  /**
   * Gets the contents of the shard which are adjacent to some other shard
   * Returns NULL on failure
   */
  graph_shard* get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                                 graph_shard_id_t adjacent_to) {
    // not implemented
    ASSERT_TRUE(false);

    // graph_shard* shard = new graph_shard();
    graph_shard_impl shard_impl; 
    shard_impl.shard_id = adjacent_to;

    const std::vector<graph_vid_t>& vids = master2vid[shard_id]; 
    for (size_t i = 0; i < vids.size(); i++) {
      if (vid2mirrors[vids[i]].find(adjacent_to) != vid2mirrors[vids[i]].end()) {
        std::vector<size_t> index_in;
        std::vector<size_t> index_out;
        edge_index[adjacent_to].get_edge_index(index_in, index_out, true, true, vids[i]);
        
        // copy incoming edges vids[i]
        for (size_t j = 0; j < index_in.size(); j++) {
          std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to].edge(index_in[j]);
          graph_row* data = shards[adjacent_to].edge_data(index_in[j]);
          graph_row* data_copy = new graph_row();
          data->deepcopy(*data_copy);
          shard_impl.add_edge(e.first, e.second, data_copy);
          shard_impl.edgeid.push_back(index_in[j]);
        }

        // copy outgoing edges of vids[i]
        for (size_t j = 0; j < index_out.size(); j++) {
          std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to].edge(index_out[j]);
          graph_row* data = shards[adjacent_to].edge_data(index_out[j]);
          graph_row* data_copy = new graph_row();
          data->deepcopy(*data_copy);
          shard_impl.add_edge(e.first, e.second, data_copy);
          shard_impl.edgeid.push_back(index_out[j]);
        }
      }
    }
    return new graph_shard(shard_impl);
  }

  /**
   * Frees a shard.
   */  
  void free_shard(graph_shard* shard) {
    shard->clear();
    delete(shard);
    shard = NULL;
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
    graph_shard_id_t id = shard->id();

    // commit vertex data
    for (size_t i = 0; i < shard->num_vertices(); i++) {
      graph_row* row = shard->vertex_data(i);
      for (size_t j = 0; j < row->num_fields(); j++) {
        graph_value* val = row->get_field(j);
        if (val->get_modified()) {
          val->post_commit_state();
        }
      }
    }

    bool commit_to_remote = shard->shard_impl.edgeid.size() > 0;
    // commit edge data
    for (size_t i = 0; i < shard->num_edges(); i++) {
      graph_row* local = shard->edge_data(i);
      graph_row* origin = commit_to_remote ? shards[id].edge_data(shard->shard_impl.edgeid[i]) : NULL;
      for (size_t j = 0; j < local->num_fields(); j++) {
        graph_value* val = local->get_field(j);
        if (val->get_modified()) {
          val->post_commit_state();
          if (origin != NULL) {
            origin->get_field(j)->free_data();
            val->deepcopy(*origin->get_field(j));
          }
        }
      }
    }
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
    
    // assign a master shard of the vertex
    boost::hash<graph_vid_t> vid_hash;
    graph_shard_id_t master = vid_hash(vid) % num_shards(); 
    shards[master].shard_impl.add_vertex(vid, row);
    vid2master[vid] = master;
    master2vid[master].push_back(vid);

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
    size_t pos = shards[shardid].shard_impl.add_edge(source, target, row);
    _num_edges++;

    // update_edge_index
    edge_index[shardid].add_edge(source, target, pos);

    // Add vertices to master shards 
    if (!vertex_index.has_vertex(source)) {
      add_vertex(source);
    }
    if (!vertex_index.has_vertex(target)) {
      add_vertex(target);
    }

    // Add vertices to mirror shards 
    if ((vid2master[source] != shardid) && 
        (vid2mirrors[source].find(shardid) == vid2mirrors[source].end())) {
    //   shards[shardid].add_vertex(source, vertex_store[vertex_index.get_index(source)]);
      vid2mirrors[source].insert(shardid);
    }
    if ((vid2master[target] != shardid) && 
        (vid2mirrors[target].find(shardid) == vid2mirrors[target].end())) {
    //   shards[shardid].add_vertex(target, vertex_store[vertex_index.get_index(target)]);
      vid2mirrors[target].insert(shardid);
    }
  }
};
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
