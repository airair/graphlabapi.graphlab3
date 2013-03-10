#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
namespace graphlab {


/**
 * \ingroup group_graph_database
 * An abstract interface for a graph database implementation 
 */
class graph_database {
 public:
  
  graph_database() { }
  virtual ~graph_database() { }

  /**
   * Returns the number of vertices in the graph.
   * This may be slow.
   */
  virtual uint64_t num_vertices() const = 0;
  
  /**
   * Returns the number of edges in the graph.
   * This may be slow.
   */
  virtual uint64_t num_edges() const = 0;

  /**
   * Returns the field metadata for the vertices in the graph
   */
  virtual const std::vector<graph_field> get_vertex_fields() const = 0;

  /**
   * Returns the field metadata for the edges in the graph
   */
  virtual const std::vector<graph_field> get_edge_fields() const = 0;

  /**
   * Returns the index of the vertex column with the given field name. 
   *
   * \note For database implementors: A default implementation using a linear 
   * search of \ref get_edge_fields() is provided.
   */
  virtual int find_vertex_field(const char* fieldname) {
    const std::vector<graph_field>& vfields = get_vertex_fields();
    for (size_t i = 0;i < vfields.size(); ++i) {
      if (vfields[i].name.compare(fieldname) == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Returns the index of the edge column with the given field name. 
   *
   * \note For database implementors: A default implementation using a linear 
   * search of \ref get_edge_fields() is provided.
   */
  virtual int find_edge_field(const char* fieldname) {
    const std::vector<graph_field>& efields = get_edge_fields();
    for (size_t i = 0;i < efields.size(); ++i) {
      if (efields[i].name.compare(fieldname) == 0) {
        return i;
      }
    }
    return -1;
  }


  // -------- Fine grained API ------------

  /** 
   * returns a vertex in ret_vertex for a queried vid in the shard with shardid.
   * Returns NULL on failure
   * The returned vertex pointer must be freed using free_vertex
   */
  virtual graph_vertex* get_vertex(graph_vid_t vid, graph_shard_id_t shardid) = 0;

  /** returns an edge in ret_edge for a queried edge id and shardid. Returns NULL on failure
   * The returned edge pointer must be freed using free_edge
   */
  virtual graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid) = 0;

  /** Gets part of the adjacency list of vertex vid belonging to shard shard_id.
   *  Returns NULL on failure. The returned edges must be freed using
   *  graph_database::free_edge() for graph_database::free_edge_vector()
   */
  virtual void get_adj_list(graph_vid_t vid, graph_shard_id_t shard_id,
                    bool prefetch_data,
                    std::vector<graph_edge*>* out_inadj,
                    std::vector<graph_edge*>* out_outadj) = 0; 

  /**
   *  Finds a vertex using an indexed integer field. Returns the vertex IDs
   *  in out_vids corresponding to the vertices where the integer field 
   *  identified by fieldpos has the specified value.
   *  Return true on success, and false on failure.
   */
  virtual bool find_vertex(size_t fieldpos,
                           graph_int_t value, 
                           std::vector<graph_vid_t>* out_vids) = 0;

  /**
   *  Finds a vertex using an indexed string field. Returns the vertex IDs
   *  in out_vids corresponding to the vertices where the string field 
   *  identified  by fieldpos has the specified value.
   *  Return true on success, and false on failure.
   */
  virtual bool find_vertex(size_t fieldpos,
                           graph_string_t value, 
                           std::vector<graph_vid_t>* out_vids) = 0;

  /**
   * Frees a vertex object
   */
  virtual void free_vertex(graph_vertex* vertex) = 0;

  /**
   * Frees a single edge object
   */
  virtual void free_edge(graph_edge* edge) = 0;
  
  /**
   * Frees a collection of edges. The vector will be cleared. on return.
   */
  virtual void free_edge_vector(std::vector<graph_edge*>& edgelist) = 0;


//  ------ Coarse Grained API ---------

  /**
   * Returns the number of shards in the database
   */
  virtual size_t num_shards() const = 0;

  /**
   * Returns the list of shard ids in the database
   */
  virtual std::vector<graph_shard_id_t> get_shard_list() const = 0;

  /**
   * Synchronously obtains a shard from the database.
   * Returns NULL on failure
   */
  virtual graph_shard* get_shard(graph_shard_id_t shard_id) = 0;
                          
  /**
   * Gets the contents of the shard which are adjacent to some other shard on local.
   * Returns NULL on failure
   */
  virtual graph_shard* get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                                 graph_shard_id_t adjacent_to) = 0;

  /**
   * Gets the contents of the shard which are adjacent to the list of vertex ids. 
   * Returns NULL on failure
   */
  virtual graph_shard* get_shard_contents_adj_to(const std::vector<graph_vid_t>& vids,
                                                 graph_shard_id_t adjacent_to) = 0;

  /**
   * Frees a shard.
   */  
  virtual void free_shard(graph_shard* shard) = 0;
  

  /**
   * Commits all the changes made to the vertex data and edge data 
   * in the shard, resetting all modification flags.
   */
  virtual void commit_shard(graph_shard* shard) = 0;


  // --------------------- Structure Modification API ----------------------
  
  virtual bool add_vertex(graph_vid_t vid, graph_shard_id_t master, graph_row* data=NULL) = 0;

  virtual bool add_edge(graph_vid_t source, graph_vid_t target,
                        graph_shard_id_t shard_id, graph_row* data=NULL) = 0;

  virtual bool add_vertex_mirror(graph_vid_t vid, graph_shard_id_t master,
                                 graph_shard_id_t mirror) = 0;
};


} // namespace graphlab

#endif
