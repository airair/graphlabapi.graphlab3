#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_IMPL_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_IMPL_HPP 
#include <utility>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>

namespace graphlab {

/**
 * \ingroup group_graph_database
 * The private contents of the graph_shard.
 */
struct graph_shard_impl {
/** 
   * the ID of the current shard 
   */
  graph_shard_id_t shard_id;
  /** 
   * The number of vertices in this shard
   */
  size_t num_vertices; 
  
  /**
   * The number of edges in this shard
   */
  size_t num_edges;

  /**
   * An array of the vertex IDs in this shard. 
   * The array has num_vertices elements
   */
  std::vector<graph_vid_t> vertex;

  /**
   * An array of all the vertex data in this shard.
   * The array has num_edges elements
   */
  std::vector<graph_row*> vertex_data;

  /**
   * A array of length num_vertices where <code>num_out_edges[i]</code> is the number 
   * number of out edges of vertex <code>vertices[i]</code> in the graph.
   */
  std::vector<size_t> num_out_edges;

  /**
   * A array of length num_vertices where <code>num_in_edges[i]</code> is the number 
   * number of in edges of vertex <code>vertices[i]</code> in the graph.
   */
  std::vector<size_t> num_in_edges;

  /**
   * An array of length num_edges. Listing for each edge in the shard, 
   * its source and target vertices. The data for edge i is stored in 
   * edge_data[i] .
   */
  std::vector< std::pair<graph_vid_t, graph_vid_t> > edge;

  /**
   * An array of length num_edges of all the edge data in the shard. 
   * This array has a 1-1 corresponding to the edges array.
   */
  std::vector<graph_row*> edge_data;

  graph_shard_impl() { 
    num_vertices = num_edges = 0;
  }

};
} // namespace graphlab
#endif
