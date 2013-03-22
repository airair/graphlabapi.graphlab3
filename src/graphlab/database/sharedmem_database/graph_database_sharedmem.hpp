#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
  /**
   * \ingroup group_graph_database
   * An shared memory implementation of a graph database.  
   * This class implements the <code>graph_database</code> interface
   * as a shared memory instance.
   * A sharedmemory database can server only one or multiple shards.
   */
  class graph_database_sharedmem : public graph_database {

    // Schema for vertex and edge datatypes
    std::vector<graph_field> vertex_fields;
    std::vector<graph_field> edge_fields;

    // Simulates backend shard storage
    graph_shard* shardarr;

    // Map from shard id to shard pointer
    boost::unordered_map<graph_shard_id_t, graph_shard*> shards;

    // A list of shard ids hosted in the database
    std::vector<graph_shard_id_t> shard_list;

   public:
    /**
     * Creates a shared memory graph database with fixed vertex and edge schemas. 
     * The database hold shards from 0 to numshards-1.
     */
    graph_database_sharedmem(const std::vector<graph_field>& vertex_fields,
                             const std::vector<graph_field>& edge_fields,
                             size_t numshards);
    /**
     * Creates a shared memory graph database with fixed vertex and edge schemas. 
     * The database hold shards whose id is from the hosted_shards.
     */
    graph_database_sharedmem(const std::vector<graph_field>& vertex_fields,
                             const std::vector<graph_field>& edge_fields,
                             const std::vector<graph_shard_id_t>& shard_list,
                             size_t numshards);

    /**
     * Destroy the database, free all vertex and edge data from memory.
     */
    inline virtual ~graph_database_sharedmem() {
      delete[] shardarr;
    }

    /**
     * Returns the number of vertices in the local database.
     * This may be slow.
     */
    inline uint64_t num_vertices() const {
      size_t ret = 0;
      for (size_t i = 0; i < num_shards(); i++) {
        ret += shardarr[i].num_vertices();
      }
      return ret;
    };

    /**
     * Returns the number of edges in the graph.
     * This may be slow.
     */
    inline uint64_t num_edges() const {
      size_t ret = 0;
      for (size_t i = 0; i < num_shards(); i++) {
        ret += shardarr[i].num_edges();
      }
      return ret;
    }

    /**
     * Returns the field metadata for the vertices in the graph
     */
    inline const std::vector<graph_field> get_vertex_fields() const {
      return vertex_fields;
    };

    /**
     * Returns the field metadata for the edges in the graph
     */
    inline const std::vector<graph_field> get_edge_fields() const {
      return edge_fields;
    };

    /**
     * Add new field to the vertex in the graph
     */
    bool add_vertex_field(graph_field& field);

    /**
     * Remove the ith field from the vertex fields
     */
    bool remove_vertex_field(size_t);



    /**
     * Add new field to the edge in the graph
     */
    bool add_edge_field(graph_field& field);

    /**
     * Remove the ith field from the edge fields
     */
    bool remove_edge_field(size_t i);


  /**
   * Set the data field at fieldpos of row with the new value. If the delta flag   
   * is true, the assignment is +=.
   * Return false on failure.
   */
    bool set_field(graph_row* row, size_t fieldpos,
                         const graph_value& new_value, bool delta);


    bool reset_field(bool is_vertex, size_t fieldpos, std::string& value);

    template<typename TransformType>
    void transform_vertices(TransformType transform_functor) {
      std::vector<graph_shard_id_t> shard_list = get_shard_list();
      for (size_t i = 0; i < shard_list.size(); i++) {
        graph_shard* shard = get_shard(shard_list[i]);
        graph_shard_impl& shard_impl = shard->shard_impl;
        for (size_t j = 0; j < shard->num_vertices(); j++) {
          graph_row& row = shard_impl.vertex_data[j];
          transform_functor(row);
        }
      }
    }

    template<typename TransformType>
    void transform_edges(TransformType transform_functor) {
      std::vector<graph_shard_id_t> shard_list = get_shard_list();
      for (size_t i = 0; i < shard_list.size(); i++) {
        graph_shard* shard = get_shard(shard_list[i]);
        graph_shard_impl& shard_impl = shard->shard_impl;
        for (size_t j = 0; j < shard->num_edges(); j++) {
          graph_row& row = shard_impl.edge_data[j];
          transform_functor(row);
        }
      }
    }

    // -------- Fine grained API ------------
    inline size_t num_in_edges(graph_vid_t vid, graph_shard_id_t shardid) {
      graph_shard* shard = get_shard(shardid);
      ASSERT_TRUE(shard != NULL);
      return shard->shard_impl.edge_index.num_in_edges(vid);
    } 

    inline size_t num_out_edges(graph_vid_t vid, graph_shard_id_t shardid) {
      graph_shard* shard = get_shard(shardid);
      ASSERT_TRUE(shard != NULL);
      return shard->shard_impl.edge_index.num_out_edges(vid);
    } 

    /**
     * Returns a graph_vertex object for the queried vid. Returns NULL on failure
     * The vertex data is passed eagerly as a pointer. Adjacency information is passed through the <code>edge_index</code>. 
     * The returned vertex pointer must be freed using free_vertex
     */
    graph_vertex* get_vertex(graph_vid_t vid, graph_shard_id_t shardid);

    /**
     * Returns a graph_edge object for quereid eid, and shardid. Returns NULL on failure.
     * The edge data is passed eagerly as a pointer. 
     * The returned edge pointer must be freed using free_edge.
     */
    graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid);

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
                      std::vector<graph_edge*>* out_outadj);


    /**
     *  Finds a vertex using an indexed integer field. Returns the vertex IDs
     *  in out_vids corresponding to the vertices where the integer field 
     *  identified by fieldpos has the specified value.
     *  Return true on success, and false on failure.
     */
    inline bool find_vertex(size_t fieldpos,
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
    inline bool find_vertex(size_t fieldpos,
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
    inline void free_vertex(graph_vertex* vertex) {
      delete vertex;
      vertex = NULL;
    };

    /**
     * Frees a single edge object.
     * The associated data is not freed. 
     */
    inline void free_edge(graph_edge* edge) {
      delete edge;
      edge = NULL;
    }

    /**
     * Frees a collection of edges. The vector will be cleared on return.
     */
    inline void free_edge_vector(std::vector<graph_edge*>& edgelist) {
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
    inline size_t num_shards() const { return shard_list.size(); }

    /**
     * Returns the list of shard ids.
     */
    inline std::vector<graph_shard_id_t> get_shard_list() const { return shard_list; }

    /**
     * Returns a reference of the shard from storage.
     * Returns NULL if the shard with shard_id is not local.
     */
    inline graph_shard* get_shard(graph_shard_id_t shard_id) {
      return shards[shard_id];
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
                                           graph_shard_id_t adjacent_to);

    graph_shard* get_shard_contents_adj_to(const std::vector<graph_vid_t>& vids, 
                                           graph_shard_id_t adjacent_to);

    /**
     * Returns a deep copy of the shard from storage.
     * The returned pointer should be freed by <code>free_shard</code>
     */
    graph_shard* get_shard_copy(graph_shard_id_t shard_id); 

    // /**
    //  * Commits all the changes made to the vertex data and edge data 
    //  * in the shard, resetting all modification flags.
    //  */
    // void commit_shard(graph_shard* shard);

    /**
     * Frees a shard. Frees all edge and vertex data from the memory. 
     * All pointers to the data in the shard will be invalid. 
     */  
    inline void free_shard(graph_shard* shard) {
      delete(shard);
      shard = NULL;
    }

    // ----------- Ingress API -----------------
    /*
     * Insert the vertex v into a shard = hash(v) as master
     * The insertion updates the global <code>vertex_store</code> as well as the master shard.
     * The corresponding <code>vid2master</code> and <code>vertex_index</code> are updated.
     * Return false if data is not NULL and v is already inserted with non-empty value.
     */
    bool add_vertex(graph_vid_t vid, graph_shard_id_t master, graph_row* data=NULL); 

    /**
     * Insert an edge from source to target with given value.
     * This will add vertex mirror info to the master shards if they were not added before.
     * The corresponding vertex mirrors and edge index are updated.
     */
    bool add_edge(graph_vid_t source, graph_vid_t target, graph_shard_id_t shard_id, graph_row* data=NULL); 

    bool add_vertex_mirror (graph_vid_t vid, graph_shard_id_t master, graph_shard_id_t mirror_shard);

    inline void add_field_helper(graph_row& row, graph_field& field) {
      row.add_field(field);
    }

    inline void remove_field_helper(graph_row& row, size_t fieldpos) {
      row.remove_field(fieldpos);
    }

    inline bool reset_field_helper(graph_row& row, size_t fieldpos,
                                    std::string& value_str) {
        graph_value* val = row.get_field(fieldpos);
        if (val != NULL) {
          return val->set_val(value_str); 
        } else {
          return false;
        }
    }
  };
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
