#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_IMPL_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_IMPL_HPP 
#include <utility>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_vertex_index.hpp>
#include <graphlab/database/graph_edge_index.hpp>
#include <boost/unordered_set.hpp>

namespace graphlab {

/**
 * \ingroup group_graph_database
 * The private contents of the graph_shard.
 *
 * This object is responsible for allocating and deleting
 * the edge data as well as the vertex data it masters.
 *
 * \note
 *  This object is not thread safe and may not be copied.
 *
 * \note
 *  A deepcopy of this object can be made on purpose by calling <code>deepcopy()</code>.
 *  The copied shard content will have the same shard id, and a copy of edge/vertex data.
 *
 * \note
 *  <code>edgeid</code> is used to maintian the shard specific edge id.
 *  Normally, the <code>edgeid</code> is the same as the index of the edge, thus is not instantiated eagerly. 
 *  When a subset of edges in this shard are selected to form a new shard (for example through <code>graph_database_sharedmem::get_adjacent_content()</code>),  the edgeid[i] for the ith edge in the new shard is equal to the index of that edge in the parent shard. This internal id relative to the parent edge is useful when committing the changes from child to its parent.
 *
 */
struct graph_shard_impl {

  /**
   * Creates an empty shard.
   */
  graph_shard_impl() {
    shard_id = -1;
    num_vertices = num_edges = 0;
    _vdata_capacity = 1000;
    _edata_capacity = 10000;
    vertex_data = new graph_row[_vdata_capacity];
    edge_data = new graph_row[_edata_capacity];
  }

  /**
   * Deconstructor. Free the edge and vertex data.
   */
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

  graph_edge_index edge_index;

  graph_vertex_index vertex_index;

  /**
   * An array of length num_vertices where vertex_mirrors[i] stores 
   * the mirrors of vertex[i].
   */ 
  std::vector<boost::unordered_set<graph_shard_id_t> > vertex_mirrors;



// ----------- Serialization API ----------------

  void save(oarchive& oarc) const {
    oarc << shard_id
         << num_vertices
         << num_edges;

    oarc << vertex;
    for (size_t i = 0; i < num_vertices; ++i)
      oarc << vertex_data[i];

    oarc << edgeid << edge;
    for (size_t i = 0; i < num_edges; ++i)
      oarc << edge_data[i];

    oarc << vertex_index << edge_index << vertex_mirrors;
  }
  
  void load(iarchive& iarc) {
    iarc >> shard_id
         >> num_vertices
         >> num_edges;

    iarc >> vertex;
    vertex_data = new graph_row[num_vertices];
    for (size_t i = 0; i < num_vertices; ++i)
      iarc >> vertex_data[i];

    iarc >> edgeid >> edge;
    edge_data = new graph_row[num_edges];
    for (size_t i = 0; i < num_edges; ++i)
      iarc >> edge_data[i];

    iarc >> vertex_index >> edge_index >> vertex_mirrors;
  }

// ----------- Modification API -----------------

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
   * For optimization purpose, the data ownership of row is transfered. 
   * */
  inline size_t add_vertex(graph_vid_t vid, graph_row* row) {
    ASSERT_TRUE(row->_own_data);
    size_t pos = num_vertices;
    vertex.push_back(vid);
    vertex_mirrors.push_back(boost::unordered_set<graph_shard_id_t>());

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

    // update vertex index
    vertex_index.add_vertex(vid, &vertex_data[pos], pos);

    return pos;
  }


  /**
   */
  inline void add_vertex_mirror(graph_vid_t v, graph_shard_id_t mirror_id) {
    if (mirror_id == shard_id)
      return;
    size_t pos = vertex_index.get_index(v);
    vertex_mirrors[pos].insert(mirror_id);
  }

  /**
   * Insert a (source, target, row) into the shard. Return the position of the edge in the shard.
   * For optimization purpose, the data ownership of row is transfered.
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
    edge_index.add_edge(source,target, pos);
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
    out.edge = edge;
    out.edgeid = edgeid;
    out.vertex_index = vertex_index;
    out.edge_index = edge_index;
    out.vertex_mirrors = vertex_mirrors;

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
