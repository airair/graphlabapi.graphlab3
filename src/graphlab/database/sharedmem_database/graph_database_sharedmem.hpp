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
  graph_shard* shardarr;

  // Map from shard id to shard pointer
  boost::unordered_map<graph_shard_id_t, graph_shard*> shards;
  
  // Dependencies between shards
  sharding_constraint sharding_graph;

  size_t _num_shards;

 public:
  /**
   * Creates a shared memory graph database with fixed vertex and edge schemas. 
   * Shards are constructed with a grid dependency.
   * All shards are hosted in the database
   */
   graph_database_sharedmem(const std::vector<graph_field>& vertex_fields,
                            const std::vector<graph_field>& edge_fields,
                            size_t numshards) : 
       vertex_fields(vertex_fields), edge_fields(edge_fields), sharding_graph(numshards, "grid"),
 _num_shards(numshards) { 
     shardarr = new graph_shard[numshards];
     for (size_t i = 0; i < numshards; i++) {
       shardarr[i].shard_impl.shard_id = i;
       shards[i] = &shardarr[i];
     }
   }

  /**
   * Creates a shared memory graph database with fixed vertex and edge schemas. 
   * Shards are constructe with a grid dependency. 
   * Onlly a subset of shards, provided by shardids, are hosted in the database. 
   */
   graph_database_sharedmem(const std::vector<graph_field>& vertex_fields,
                            const std::vector<graph_field>& edge_fields,
                            const std::vector<graph_shard_id_t>& hosted_shards,
                            size_t numshards) : 
       vertex_fields(vertex_fields), edge_fields(edge_fields), sharding_graph(numshards, "grid"),
 _num_shards(hosted_shards.size()) { 
     shardarr = new graph_shard[hosted_shards.size()];
     for (size_t i = 0; i < hosted_shards.size(); i++) {
       shardarr[i].shard_impl.shard_id = hosted_shards[i];
       shards[hosted_shards[i]] = &shardarr[i];
     }
   }

   /**
    * Destroy the database, free all vertex and edge data from memory.
    */
   virtual ~graph_database_sharedmem() {
     delete[] shardarr;
   }

  /**
   * Returns the number of vertices in the local database.
   * This may be slow.
   */
  uint64_t num_vertices() {
    size_t ret = 0;
    for (size_t i = 0; i < _num_shards; i++) {
      ret += shardarr[i].num_vertices();
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
      ret += shardarr[i].num_edges();
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
  /**
   * Returns a graph_vertex object for the queried vid. Returns NULL on failure
   * The vertex data is passed eagerly as a pointer. Adjacency information is passed through the <code>edge_index</code>. 
   * The returned vertex pointer must be freed using free_vertex
   */
  graph_vertex* get_vertex(graph_vid_t vid) {
    graph_shard_id_t master = sharding_graph.get_master(vid);
    if (shards[master] == NULL || !shards[master]->has_vertex(vid)) {
      return NULL;
    }
    graph_row* vertex_data = shards[master]->vertex_data_by_id(vid);
    const boost::unordered_set<graph_shard_id_t>& mirrors = shards[master]->get_mirrors(vid);
    ASSERT_TRUE(vertex_data != NULL);
    return (new graph_vertex_sharedmem(vid, vertex_data, master, mirrors, this));
  };

  /**
   * Returns a graph_edge object for quereid eid, and shardid. Returns NULL on failure.
   * The edge data is passed eagerly as a pointer. 
   * The returned edge pointer must be freed using free_edge.
   */
  graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid) {
    if (shards[shardid] == NULL || eid >= shards[shardid]->num_edges()) {
      return NULL;
    } else {
      return (new graph_edge_sharedmem(eid, shardid, this));
    } 
  }

  /** Gets part of the adjacency list of vertex vid belonging to shard shard_id.
   *  The shardid must be a local shard.
   *  Returns NULL on failure. The returned edges must be freed using
   *  graph_database::free_edge() for graph_database::free_edge_vector()
   *
   *  out_inadj will be filled to contain a list of graph edges where the 
   *  destination vertex is the current vertex. out_outadj will be filled to
   *  contain a list of graph edges where the source vertex is the current 
   *  vertex.
   *
   *  Either out_inadj or out_outadj may be NULL in which case those edges
   *  are not retrieved (for instance, I am only interested in the in edges of 
   *  the vertex).
   *
   *  If prefetch_data does not have effect. 
   */ 
  void get_adj_list(graph_vid_t vid, graph_shard_id_t shard_id,
                    bool prefetch_data,
                    std::vector<graph_edge*>* out_inadj,
                    std::vector<graph_edge*>* out_outadj) {
    graph_shard* shard = get_shard(shard_id);
    ASSERT_TRUE(shard != NULL);
    std::vector<size_t> index_in;
    std::vector<size_t> index_out;
    bool getIn = !(out_inadj == NULL);
    bool getOut = !(out_outadj == NULL);
    shard->shard_impl.edge_index.get_edge_index(index_in, index_out, getIn, getOut, vid);

    if (getIn) {
      graph_edge_sharedmem* inadj = new graph_edge_sharedmem[index_in.size()];
      for(size_t i = 0; i < index_in.size(); i++) {
        std::pair<graph_vid_t, graph_vid_t> pair = shard->edge(index_in[i]);
        inadj[i].eid = index_in[i];
        inadj[i].master = shard_id;
        inadj[i].database = this;
        out_inadj->push_back(&inadj[i]);
      }
    }

    if (getOut) {
      graph_edge_sharedmem* outadj = new graph_edge_sharedmem[index_out.size()];
      for (size_t i = 0; i < index_out.size(); i++) {  
        std::pair<graph_vid_t, graph_vid_t> pair = shard->edge(index_out[i]);
        outadj[i].eid = index_out[i];
        outadj[i].master = shard_id;
        outadj[i].database = this;
        out_outadj->push_back(&outadj[i]);
      }
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
  void free_edge_vector(std::vector<graph_edge*>& edgelist) {
    if (edgelist.size() == 0)
      return;
    graph_edge_sharedmem* head = (graph_edge_sharedmem*)edgelist[0];
    delete[] head;
    edgelist.clear();
  }


//  ------ Coarse Grained API ---------

  /**
   * Returns the number of shards in the database
   */
  size_t num_shards() { return sharding_graph.num_shards(); }
  
  /**
   * Returns a reference of the shard from storage.
   * Returns NULL if the shard with shard_id is not local.
   */
  graph_shard* get_shard(graph_shard_id_t shard_id) {
    return shards[shard_id];
  }

  /**
   * Returns a deep copy of the shard from storage.
   * The returned pointer should be freed by <code>free_shard</code>
   */
  graph_shard* get_shard_copy(graph_shard_id_t shard_id) {
    if (shards[shard_id] == NULL) {
      return NULL;
    }
    graph_shard* ret = new graph_shard;
    shards[shard_id]->shard_impl.deepcopy(ret->shard_impl);
    return ret;
  }

                          
  /**
   * Gets the contents of the shard which are adjacent to some other shard.
   * Creats a new shard with only the relevant edges, and no vertices.
   * It makes a copy of the edge data from the original shard, and fills in the <code>shard_impl.edgeid</code>
   * with the index from the original shard.
   *
   * Assuming both shards exists. Returns NULL on failure.
   */
  graph_shard* get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                                 graph_shard_id_t adjacent_to) {
    if (shards[shard_id] == NULL || shards[adjacent_to] == NULL) {
      return NULL;
    }

      const std::vector<graph_vid_t>& vids = shards[shard_id]->shard_impl.vertex;
      return get_shard_contents_adj_to(vids, adjacent_to);
    }


  graph_shard* get_shard_contents_adj_to(const std::vector<graph_vid_t>& vids, 
                                         graph_shard_id_t adjacent_to) {

      graph_shard* ret = new graph_shard();
      graph_shard_impl& shard_impl = ret->shard_impl;
      shard_impl.shard_id = adjacent_to;

      boost::unordered_set<graph_eid_t> eids;

    // For each vertex in vids, if its master or mirrors conatins adjacent_to, then copy its adjacent edges from adjacent_to. 
    for (size_t i = 0; i < vids.size(); i++) {
        // if (shards[adjacent_to]->has_vertex(vids[i]) || 
        //     (shards[adjacent_to]->get_mirrors(vids[i]).find(adjacent_to)
        //      != shards[adjacent_to]->get_mirrors(vids[i]).end())) {
        std::vector<size_t> index_in;
        std::vector<size_t> index_out;
        shards[adjacent_to]->
            shard_impl.edge_index.get_edge_index(index_in, index_out, true, true, vids[i]);
        
        // copy incoming edges vids[i]
        for (size_t j = 0; j < index_in.size(); j++) {
          // avoid adding the same edge twice
          if (eids.find(index_in[j]) == eids.end()) { 
            std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to]->edge(index_in[j]);
            graph_row* data = shards[adjacent_to]->edge_data(index_in[j]);
            graph_row* data_copy = new graph_row();
            data->deepcopy(*data_copy);
            shard_impl.add_edge(e.first, e.second, data_copy);
            shard_impl.edgeid.push_back(index_in[j]);
            eids.insert(index_in[j]);
            delete data_copy;
          }
        }

        // copy outgoing edges of vids[i]
        for (size_t j = 0; j < index_out.size(); j++) {
          // avoid adding the same edge twice
          if (eids.find(index_out[j]) == eids.end()) {
            std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to]->edge(index_out[j]);
            graph_row* data = shards[adjacent_to]->edge_data(index_out[j]);
            graph_row* data_copy = new graph_row();
            data->deepcopy(*data_copy);
            shard_impl.add_edge(e.first, e.second, data_copy);
            shard_impl.edgeid.push_back(index_out[j]);
            eids.insert(index_out[j]);
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
    ASSERT_TRUE(shards[id] != NULL);

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
      graph_row* origin = derivedshard ? shards[id]->edge_data(shard->shard_impl.edgeid[i]) 
                                       : shards[id]->edge_data(i);
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

// ----------- Ingress API -----------------
  /*
   * Insert the vertex v into a shard = hash(v) as master
   * The insertion updates the global <code>vertex_store</code> as well as the master shard.
   * The corresponding <code>vid2master</code> and <code>vertex_index</code> are updated.
   * Return false if data is not NULL and v is already inserted with non-empty value.
   */
  bool add_vertex(graph_vid_t vid, graph_shard_id_t master, graph_row* data=NULL) {
    ASSERT_TRUE(shards[master] != NULL);

    if (shards[master]->has_vertex(vid)) { // vertex has already been inserted 
      if (data) {
        graph_row* row =  shards[master]->vertex_data_by_id(vid);
        if (row->is_null()) { // existing vertex has no value, update with new value
          data->copy_transfer_owner(*row);
          delete data;
          return true;
        } else { // existing vertex has value, cannot overwrite, return false
          return false;
        }
      } else { // new insertion does not have value, do nothing and return true;
        return true;
      }
    } else {
      // create a new row of all null values.
      graph_row* row = (data==NULL) ? new graph_row(vertex_fields, true) : data;
      row->_is_vertex = true;
      
      // Insert into shard. This operation transfers the data ownership to the row in the shard
      // so that we can free the row at the end of the function.
      shards[master]->shard_impl.add_vertex(vid, row);
      return true;
    }
  }

  /**
   * Insert an edge from source to target with given value.
   * This will add vertex mirror info to the master shards if they were not added before.
   * The corresponding vertex mirrors and edge index are updated.
   */
  void add_edge(graph_vid_t source, graph_vid_t target, graph_shard_id_t shard_id, graph_row* data=NULL) {
    ASSERT_TRUE(shards[shard_id] != NULL);

    // create a new row of all null values
    graph_row* row = (data==NULL) ? new graph_row(edge_fields, false) : data;

    // Insert into shard. This operation transfers the data ownership to the row in the shard
    // so that we can free the row at the end of the function.
    shards[shard_id]->shard_impl.add_edge(source, target, row);
  }

  /**
   * Add shard_id to the vertex mirror list.
   * Assuming the vertex to be updated is stored in a local shard.
   */
  void add_vertex_mirror (graph_vid_t vid, graph_shard_id_t master, graph_shard_id_t mirror_shard) {
    ASSERT_TRUE(shards[master] != NULL);
    if (!shards[master]->has_vertex(vid)) {
      add_vertex(vid, master);
    }
    shards[master]->shard_impl.add_vertex_mirror(vid, mirror_shard);
  }
};
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
