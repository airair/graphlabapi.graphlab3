#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_IMPL_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_IMPL_HPP 
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
  inline graph_shard_impl(): shard_id(-1), num_vertices(0), num_edges(0),
                      _vdata_capacity(1000), _edata_capacity(10000),
                      vertex_data(new graph_row[_vdata_capacity]),
                      edge_data(new graph_row[_edata_capacity]) { }

  /**
   * Deconstructor. Free the edge and vertex data.
   */
  inline ~graph_shard_impl() {
    if (vertex_data != NULL)
      delete[] vertex_data;
    if (edge_data != NULL)
      delete[] edge_data;
    vertex_data = edge_data = NULL;
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
   * Index for adjacency structure lookup.
   */
  graph_edge_index edge_index;

  /**
   * Index for vertex lookup.
   */
  graph_vertex_index vertex_index;

  /**
   * An array of length num_vertices where vertex_mirrors[i] stores 
   * the mirrors of vertex[i].
   */ 
  std::vector<boost::unordered_set<graph_shard_id_t> > vertex_mirrors;



// ----------- Serialization API ----------------

  void save(oarchive& oarc) const;
  
  void load(iarchive& iarc);
  
// ----------- Modification API -----------------
  /**
   * Insert a (vid, row) into the shard. Return the position of the vertex in the shard.
   * For optimization, take over the row pointer. 
   * */
  size_t add_vertex(graph_vid_t vid, graph_row* row);
  /**
   * Insert a (vid, mirror) record into the shard. Shard must be the master of the vertex.
   */
  void add_vertex_mirror(graph_vid_t v, graph_shard_id_t mirror_id);

  /**
   * Insert a (source, target, row) into the shard. Return the position of the edge in the shard.
   * For optimization purpose, the data ownership of row is transfered.
   * */
  size_t add_edge(graph_vid_t source, graph_vid_t target, graph_row* row);

  /**
   * Make a deep copy of this shard into out.
   */
  void deepcopy(graph_shard_impl& out);

 private:
  // copy constructor deleted. It is not safe to copy this object.
  graph_shard_impl(const graph_shard_impl&) { }

  // assignment operator deleted. It is not safe to copy this object.
  graph_shard_impl& operator=(const graph_shard_impl&) { return *this; }


};
} // namespace graphlab
#endif
