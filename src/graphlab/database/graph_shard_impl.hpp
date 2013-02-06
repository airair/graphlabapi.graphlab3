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
  graph_shard_impl() {
    num_vertices = num_edges = 0;
  }

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
   * \note Deprecated because shard only store master vertices.
   */
  // std::vector<size_t> num_out_edges;

  /**
   * An array of length num_vertices where <code>num_in_edges[i]</code> is the number 
   * number of in edges of vertex <code>vertices[i]</code> in the graph.
   * \note Deprecated because shard only store master vertices.
   */
  // std::vector<size_t> num_in_edges;


  /**
   * An array of length num_edges where edgeid[i] is the internal edge id (relevant to shard)
   * of edge[i]. In a full shard, the edgeid array is a lazy. In a derived, shard, the id will be filled in properly.
   */
  std::vector<graph_eid_t> edgeid;

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

  /**
   * Insert a (vid, pair) into the shard. Return the position of the vertex in the shard.
   * */
  inline size_t add_vertex(graph_vid_t vid, graph_row* data) {
    size_t pos = num_vertices++;
    vertex.push_back(vid);
    vertex_data.push_back(data);
    return pos;
  }

  /**
   * Insert a (source, target, data) into the shard. Return the position of the edge in the shard.
   * */
  inline size_t add_edge(graph_vid_t source, graph_vid_t target, graph_row* data) {
    size_t pos = num_edges++;
    edge.push_back(std::pair<graph_vid_t, graph_vid_t>(source, target));
    edge_data.push_back(data);
    return pos;
  }


  /**
   * Make a deep copy of this shard into out.
   */
  void deepcopy(graph_shard_impl& out) {
    out.shard_id = shard_id;
    out.num_vertices = num_vertices;
    out.num_edges = num_edges;
    out.vertex = vertex;
    // out.num_out_edges = num_out_edges;
    // out.num_in_edges = num_in_edges;
    out.edge = edge;
    out.edgeid = edgeid;

    // make a deep copy of all edge data.
    out.edge_data.resize(edge_data.size());
    graph_row* new_edge_data = (graph_row*)(malloc(sizeof(graph_row)*edge_data.size()));
    for (size_t i = 0; i < edge_data.size(); i++) {
      edge_data[i]->deepcopy(new_edge_data[i]);
      out.edge_data[i] = new_edge_data + i;
    }

    // make a deep copy of all vertexdata.
    out.vertex_data.resize(vertex_data.size());
    graph_row* new_vertex_data = (graph_row*)(malloc(sizeof(graph_row)*vertex_data.size()));
    for (size_t i = 0; i < vertex_data.size(); i++) {
      vertex_data[i]->deepcopy(new_vertex_data[i]);
      out.vertex_data[i] = new_vertex_data + i;
    }
  }
};
} // namespace graphlab
#endif
