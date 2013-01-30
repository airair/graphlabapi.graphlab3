#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_vertex_index.hpp>
#include <graphlab/database/graph_edge_index.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/graph_vertex_sharedmem.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
/**
 * \ingroup group_graph_database_sharedmem
 * An shared memory implementation of a graph database
 */
class graph_database_sharedmem : public graph_database {
  std::vector<graph_field> vertex_fields;
  std::vector<graph_field> edge_fields;
  std::vector<graph_shard> shards;
  graph_vertex_index vertex_index;
  graph_edge_index edge_index;

  size_t _num_vertices;
  size_t _num_edges;
 public:
   graph_database_sharedmem(std::vector<graph_field> vertex_fields,
                            std::vector<graph_field> edge_fields,
                            size_t numshards) { }

   ~graph_database_sharedmem() {
     for (size_t i = 0; i < shards.size(); ++i)
        free(&shards[i]); 
   }

  /**
   * Returns the number of vertices in the graph.
   * This may be slow.
   */
  uint64_t num_vertices() {
    return _num_vertices;
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
    return (new graph_vertex_sharedmem(vid, &vertex_index, &edge_index, this));
  };

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
};
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
