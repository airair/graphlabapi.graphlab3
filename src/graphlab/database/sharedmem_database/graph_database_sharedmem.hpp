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
#include <graphlab/database/sharedmem_database/graph_vertex_sharedmem.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
/**
 * \ingroup group_graph_database
 * An shared memory implementation of a graph database.  
 * This class implements the <code>graph_database</code> interface
 * as a shared memory instance.
 */
class graph_database_sharedmem : public graph_database {

  // Schema for vertex and edge datatypes
  std::vector<graph_field> vertex_fields;
  std::vector<graph_field> edge_fields;

  // Simulates backend shard storage
  graph_shard* shards;

  // Dependencies between shards
  sharding_constraint sharding_graph;

  // Hash function for vertex id.
  boost::hash<graph_vid_t> vidhash;

  // Hash function for edge id.
  boost::hash<std::pair<graph_vid_t, graph_vid_t> > edge_hash;

  // Edge index for each shard 
  std::vector<graph_edge_index*> shard_edge_index;

  size_t _num_shards;

 public:
  graph_database_sharedmem() { }
  /**
   * Creates a shared memory graph database with fixed vertex and edge schemas. 
   * Shards are constructed with a grid depedency.
   */
   graph_database_sharedmem(std::vector<graph_field> vertex_fields,
                            std::vector<graph_field> edge_fields,
                            size_t numshards) : 
       vertex_fields(vertex_fields), edge_fields(edge_fields), sharding_graph(numshards, "grid"),
 _num_shards(numshards) { 
     shards = new graph_shard[numshards];
     for (size_t i = 0; i < numshards; i++) {
       shards[i].shard_impl.shard_id = i;
       shards[i].shard_impl.num_vertices = 0;
       shards[i].shard_impl.num_edges= 0;
       shard_edge_index.push_back(&(shards[i].shard_impl.edge_index));
     }
   }

   /**
    * Destroy the database, free all vertex and edge data from memory.
    */
   virtual ~graph_database_sharedmem() {
     delete[] shards;
   }

  /**
   * Returns the number of vertices in the graph.
   * This may be slow.
   */
  uint64_t num_vertices() {
    size_t ret = 0;
    for (size_t i = 0; i < _num_shards; i++) {
      ret += shards[i].num_vertices();
    }
    return ret;
  };
  
  /**
   * Returns the number of edges in the graph.
   * This may be slow.
   */
  uint64_t num_edges() {
    size_t ret = 0;
    for (size_t i = 0; i < _num_shards; i++) {
      ret += shards[i].num_edges();
    }
    return ret;
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


  /**
   * Returns the sharding constraint graph.
   */
  const sharding_constraint& get_sharding_constraint() {
    return sharding_graph;
  }


  // -------- Fine grained API ------------
  graph_shard_id_t get_master(graph_vid_t vid) {
    return vidhash(vid) % _num_shards; 
  } 

  /**
   * Returns a graph_vertex object for the queried vid. Returns NULL on failure
   * The vertex data is passed eagerly as a pointer. Adjacency information is passed through the <code>edge_index</code>. 
   * The returned vertex pointer must be freed using free_vertex
   */
  graph_vertex* get_vertex(graph_vid_t vid) {
    graph_shard_id_t master = get_master(vid);
    if (!shards[master].has_vertex(vid)) {
      return NULL;
    }
    graph_row* vertex_data = shards[master].vertex_data_by_id(vid);
    const boost::unordered_set<graph_shard_id_t>& mirrors = shards[master].get_mirrors(vid);
    ASSERT_TRUE(vertex_data != NULL);
    return (new graph_vertex_sharedmem(vid, vertex_data, master, mirrors, shard_edge_index, this));
  };


  /**
   * Returns a graph_edge object for quereid eid, and shardid. Returns NULL on failure.
   * The edge data is passed eagerly as a pointer. 
   * The returned edge pointer must be freed using free_edge.
   */
  graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid) {
    if (shardid >= num_shards() || eid >= shards[shardid].num_edges()) {
      return NULL;
    } else {
      std::pair<graph_vid_t, graph_vid_t> pair = shards[shardid].edge(eid);
      graph_row* edge_data = shards[shardid].edge_data(eid);
      return (new graph_edge_sharedmem(pair.first, pair.second, eid, edge_data, shardid, this));
    } 
  }

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
   * Frees a vertex object.
   * The associated data is not freed. 
   */
  void free_vertex(graph_vertex* vertex) {
    delete vertex;
    vertex = NULL;
  };

  /**
   * Frees a single edge object.
   * The associated data is not freed. 
   */
  void free_edge(graph_edge* edge) {
    delete edge;
    edge = NULL;
  }
  
  /**
   * Frees a collection of edges. The vector will be cleared on return.
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
  size_t num_shards() { return _num_shards; }
  
  /**
   * Returns a reference of the shard from storage.
   */
  graph_shard* get_shard(graph_shard_id_t shard_id) {
    return &shards[shard_id];
  }

  /**
   * Returns a deep copy of the shard from storage.
   * The returned pointer should be freed by <code>free_shard</code>
   */
  graph_shard* get_shard_copy(graph_shard_id_t shard_id) {
    graph_shard* ret = new graph_shard;
    shards[shard_id].shard_impl.deepcopy(ret->shard_impl);
    return ret;
  }

                          
  /**
   * Gets the contents of the shard which are adjacent to some other shard.
   * Creats a new shard with only the relevant edges, and no vertices.
   * It makes a copy of the edge data from the original shard, and fills in the <code>shard_impl.edgeid</code>
   * with the index from the original shard.
   * Returns NULL on failure.
   */
  graph_shard* get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                                 graph_shard_id_t adjacent_to) {
    graph_shard* ret = new graph_shard();
    graph_shard_impl& shard_impl = ret->shard_impl;
    shard_impl.shard_id = adjacent_to;

    const std::vector<graph_vid_t>& vids = shards[shard_id].shard_impl.vertex;

    // For each vertex in shard_id, if its master or mirrors conatins adjacent_to, then copy its adjacent edges from adjacent_to. 
    for (size_t i = 0; i < vids.size(); i++) {
        if ((shard_id == adjacent_to) || 
            (shards[shard_id].get_mirrors(vids[i]).find(adjacent_to)
             != shards[shard_id].get_mirrors(vids[i]).end())) {

        std::vector<size_t> index_in;
        std::vector<size_t> index_out;
        shard_edge_index[adjacent_to]->get_edge_index(index_in, index_out, true, true, vids[i]);
        
        // copy incoming edges vids[i]
        for (size_t j = 0; j < index_in.size(); j++) {
          std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to].edge(index_in[j]);
          graph_row* data = shards[adjacent_to].edge_data(index_in[j]);
          graph_row* data_copy = new graph_row();
          data->deepcopy(*data_copy);
          shard_impl.add_edge(e.first, e.second, data_copy);
          shard_impl.edgeid.push_back(index_in[j]);
          delete data_copy;
        }

        // copy outgoing edges of vids[i]
        for (size_t j = 0; j < index_out.size(); j++) {
          std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to].edge(index_out[j]);
          graph_row* data = shards[adjacent_to].edge_data(index_out[j]);
          graph_row* data_copy = new graph_row();
          data->deepcopy(*data_copy);
          shard_impl.add_edge(e.first, e.second, data_copy);
          shard_impl.edgeid.push_back(index_out[j]);
          delete data_copy;
        }
      }
    }
    return ret;
  }

  /**
   * Frees a shard. Frees all edge and vertex data from the memory. 
   * All pointers to the data in the shard will be invalid. 
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

    // commit edge data
    // if the shard to commit is a derived shard, we need to overwrite 
    // the corresponding edges in the original shard
    bool derivedshard = shard->shard_impl.edgeid.size() > 0;

    for (size_t i = 0; i < shard->num_edges(); i++) {
      graph_row* local = shard->edge_data(i);

      graph_row* origin = derivedshard ? shards[id].edge_data(shard->shard_impl.edgeid[i]) : shards[id].edge_data(i);
      ASSERT_TRUE(origin != NULL);

      for (size_t j = 0; j < local->num_fields(); j++) {
        graph_value* val = local->get_field(j);
        if (val->get_modified()) {
          val->post_commit_state();
          origin->get_field(j)->free_data();
          val->deepcopy(*origin->get_field(j));
        }
      }
    }
  }

// ----------- Modification API -----------------
  /*
   * Insert the vertex v into a shard = hash(v) as master
   * The insertion updates the global <code>vertex_store</code> as well as the master shard.
   * The corresponding <code>vid2master</code> and <code>vertex_index</code> are updated.
   * Return false if v is already inserted.
   */
  bool add_vertex(graph_vid_t vid, graph_row* data=NULL) {
    // assign a master shard of the vertex
    graph_shard_id_t master = get_master(vid); 

    if (shards[master].has_vertex(vid)) {
      return false;
    }

    // create a new row of all null values.
    graph_row* row = (data==NULL) ? new graph_row(this, vertex_fields) : data;
    row->_is_vertex = true;
    
    // Insert into shard. This operation transfers the data ownership to the row in the shard
    // so that we can free the row at the end of the function.
    shards[master].shard_impl.add_vertex(vid, row);

    delete row;
    return true;
  }

  /**
   * Insert an edge from source to target with given value.
   * This will add vertex mirror info to the master shards if they were not added before.
   * The corresponding vertex mirrors and edge index are updated.
   */
  void add_edge(graph_vid_t source, graph_vid_t target, graph_row* data=NULL) {

    // Add vertices to master shards 
    if (!shards[get_master(source)].has_vertex(source)) {
      add_vertex(source);
    }
    if (!shards[get_master(target)].has_vertex(target)) {
      add_vertex(target);
    }


    std::vector<graph_shard_id_t> candidates;
    sharding_graph.get_joint_neighbors(get_master(source), get_master(target), candidates);
    ASSERT_GT(candidates.size(), 0);

    graph_shard_id_t shardid = candidates[edge_hash(std::pair<graph_vid_t, graph_vid_t>(source, target)) % candidates.size()];

    // create a new row of all null values
    graph_row* row = (data==NULL) ? new graph_row(this, edge_fields) : data;

    row->_is_vertex = false;

    // Insert into shard. This operation transfers the data ownership to the row in the shard
    // so that we can free the row at the end of the function.
    shards[shardid].shard_impl.add_edge(source, target, row);

    shards[get_master(source)].shard_impl.add_vertex_mirror(source, shardid);
    shards[get_master(target)].shard_impl.add_vertex_mirror(target, shardid);

    delete row;
  }
};
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
