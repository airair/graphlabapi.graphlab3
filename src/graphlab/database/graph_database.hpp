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
  virtual uint64_t num_vertices() = 0;
  
  /**
   * Returns the number of edges in the graph.
   * This may be slow.
   */
  virtual uint64_t num_edges() = 0;

  /**
   * Returns the field metadata for the vertices in the graph
   */
  virtual const std::vector<graph_field>& get_vertex_fields() = 0;

  /**
   * Returns the field metadata for the edges in the graph
   */
  virtual const std::vector<graph_field>& get_edge_fields() = 0;
  
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

  /** returns a vertex in ret_vertex for a queried vid. Returns NULL on failure
   * The returned vertex pointer must be freed using free_vertex
   */
  virtual graph_vertex* get_vertex(graph_vid_t vid) = 0;

  /**
   *  Finds a vertex using an indexed integer field. Returns the vertex ID
   *  in out_vid corresponding to the vertex where the integer field identified
   *  by fieldpos has the specified value.
   *  Return true on success, and false on failure.
   */
  virtual bool find_vertex(size_t fieldpos,
                           graph_int_t value, 
                           graph_vid_t* out_vid) = 0;

  /**
   *  Finds a vertex using an indexed string field. Returns the vertex ID
   *  in out_vid corresponding to the vertex where the string field identified
   *  by fieldpos has the specified value.
   *  Return true on success, and false on failure.
   */
  virtual bool find_vertex(size_t fieldpos,
                           graph_string_t value, 
                           graph_vid_t* ret_vid) = 0;

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
  virtual void free_edge_vector(std::vector<graph_edge*>* edgelist) = 0;


//  ------ Coarse Grained API ---------

  /**
   * Returns the number of shards in the database
   */
  virtual size_t num_shards() = 0;
  
  
  /**
   * Synchronously obtains a shard from the database.
   * Returns NULL on failure
   */
  virtual graph_shard* get_shard(graph_shard_id_t shard_id) = 0;
                          
  /**
   * Gets the contents of the shard which are adjacent to some other shard
   * Returns NULL on failure
   */
  virtual graph_shard* get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                                 graph_shard_id_t adjacent_to) = 0;
  /**
   * Frees a shard.
   */  
  virtual void free_shard(graph_shard* shard) = 0;
  
  /** 
   * Returns a list of shards IDs which adjacent to a given shard id
   */
  virtual void adjacent_shards(graph_shard_id_t shard_id, 
                               std::vector<graph_shard_id_t>* out_adj_shard_ids) = 0;

  /**
   * Commits all the changes made to the vertex data and edge data 
   * in the shard, resetting all modification flags.
   */
  void commit_shard(graph_shard* shard);
};


} // namespace graphlab

#endif

