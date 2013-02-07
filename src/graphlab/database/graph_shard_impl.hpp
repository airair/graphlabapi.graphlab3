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
    _vdata_capacity = 100;
    _edata_capacity = 1000;
    vertex_data = new graph_row[_vdata_capacity];
    edge_data = new graph_row[_edata_capacity];
  }

  ~graph_shard_impl() {
    delete[] vertex_data;
    delete[] edge_data;
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
   * The capacity of vertex data
   */
  size_t _vdata_capacity;
  /**
   * The initial capacity of edge data
   */
  size_t _edata_capacity;


  /**
   * An array of the vertex IDs in this shard. 
   * The array has num_vertices elements
   */
  std::vector<graph_vid_t> vertex;

  /**
   * An array of all the vertex data in this shard.
   * The array has num_edges elements
   */
  graph_row* vertex_data;

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
  graph_row* edge_data;


  /**
   * Clear all the information in the shard. Keep the shard_id.
   */
  inline void clear () {
      vertex.clear();
      edge.clear();
      edgeid.clear();
      num_vertices = num_edges = 0;
      delete[] edge_data;
      delete[] vertex_data;
      vertex_data = edge_data = NULL;
  }

  /**
   * Insert a (vid, row) into the shard. Return the position of the vertex in the shard.
   * For optimization purpose, the data owner ship of row is transfered. 
   * */
  inline size_t add_vertex(graph_vid_t vid, graph_row* row) {
    ASSERT_TRUE(row->_own_data);
    size_t pos = num_vertices;
    vertex.push_back(vid);

    // resize array, double the capacity.
    if (pos == _vdata_capacity) {
      _vdata_capacity *= 2;
      graph_row* vertex_data_tmp = new graph_row[_vdata_capacity];
      for (size_t i = 0; i < num_vertices; i++) {
        vertex_data[i].copy_transfer_owner(vertex_data_tmp[i]);
      }
      delete[] vertex_data;
      vertex_data = vertex_data_tmp;
    }

    row->copy_transfer_owner(vertex_data[pos]);
    num_vertices++;
    return pos;
  }

  /**
   * Insert a (source, target, row) into the shard. Return the position of the edge in the shard.
   * For optimization purpose, the data owner ship of row is transfered.
   * */
  inline size_t add_edge(graph_vid_t source, graph_vid_t target, graph_row* row) {
    size_t pos = num_edges;
    edge.push_back(std::pair<graph_vid_t, graph_vid_t>(source, target));

    // Resize array, double the size
    if (pos == _edata_capacity) {
      _edata_capacity *= 2;
      graph_row* edge_data_tmp = new graph_row[_edata_capacity];
      for (size_t i = 0; i < num_edges; i++) {
        edge_data[i].copy_transfer_owner(edge_data_tmp[i]);
      }
      delete[] edge_data;
      edge_data = edge_data_tmp;
    }

    row->copy_transfer_owner(edge_data[pos]);
    num_edges++;
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

    // Resize the vertex data array.
    if (out._vdata_capacity <= num_vertices) {
      delete[] out.vertex_data;
      out._vdata_capacity = num_vertices;
      out.vertex_data = new graph_row[_vdata_capacity];
    }

    // Resize the edge data array.
    if (out._edata_capacity <= num_edges) {
      delete[] out.edge_data;
      out._edata_capacity = num_edges;
      out.edge_data = new graph_row[_edata_capacity];
    }

    // make a deep copy of all vertexdata.
    for (size_t i = 0; i < num_vertices; i++) {
      vertex_data[i].deepcopy(out.vertex_data[i]);
    }

    // make a deep copy of all edge data.
    for (size_t i = 0; i < num_edges; i++) {
      edge_data[i].deepcopy(out.edge_data[i]);
    }
  }

 private:
  // copy constructor deleted. It is not safe to copy this object.
  graph_shard_impl(const graph_shard_impl&) { }

  // assignment operator deleted. It is not safe to copy this object.
  graph_shard_impl& operator=(const graph_shard_impl&) { return *this; }


};
} // namespace graphlab
#endif
