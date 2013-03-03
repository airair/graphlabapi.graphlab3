#ifndef GRAPHLAB_DATABASE_GRAPH_VERTEX_SHARED_MEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_VERTEX_SHARED_MEM_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/sharedmem_database/graph_edge_sharedmem.hpp>
#include <graphlab/database/graph_vertex_index.hpp>
#include <boost/unordered_set.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
/**
 * \ingroup group_graph_database
 *  An shared memory implementation of <code>graph_vertex</code>.
 *  The vertex data is directly accessible through pointers. 
 *  Adjacency information is accessible through <code>edge_index</code>
 *  object passed from the <code>graph_database_sharedmem</code>.
 *
 * This object is not thread-safe, and may not copied.
 */
class graph_vertex_sharedmem : public graph_vertex {
 private:

  // Id of the vertex.
  graph_vid_t vid;

  // Pointer to the vertex data in the storage.
  graph_row* vdata;

  // Master shard id of this vertex.
  graph_shard_id_t master;

  // Mirror shards spanned by this vertex.
  boost::unordered_set<graph_shard_id_t> mirrors;
  
  // Pointer to the database.
  graph_database* database;

 public:
  /**
   * Creates a graph vertex object 
   */
  graph_vertex_sharedmem(graph_vid_t vid,
                         graph_row* data,
                         graph_shard_id_t master,
                         const boost::unordered_set<graph_shard_id_t>& mirrors,
                         graph_database* db) : 
      vid(vid), vdata(data), master(master), mirrors(mirrors), database(db) {}

  /**
   * Returns the ID of the vertex
   */
  graph_vid_t get_id() const {
    return vid;
  }

  /**
   * Returns a pointer to the graph_row representing the data
   * stored on this vertex. Modifications made to the data, are only committed 
   * to the database through a write_* call.
   */
  graph_row* data() {
    if (vdata == NULL) {
      refresh();
    }
    return vdata;
  };

  // --- synchronization ---

  /**
   * Commits changes made to the data on this vertex synchronously.
   * This resets the modification and delta flags on all values in the 
   * graph_row.
   *
   * TODO: check delta commit.
   */ 
  void write_changes() {  
    for (size_t i = 0; i < vdata->num_fields(); i++) {
      graph_value* val = vdata->get_field(i);
      if (val->get_modified()) {
        val->post_commit_state();
      }
    }
  }

  /**
   * Same as synchronous commit in shared memory.
   */ 
  void write_changes_async() { 
    write_changes();
  }

  /**
   * Fetch the data pointer from the right shard. 
   */ 
  void refresh() { 
    if (vdata == NULL) {
      vdata = database->get_shard(master)->vertex_data_by_id(vid);
    }
  }

  /**
   * Commits the change immediately.
   * Refresh has no effects in shared memory.
   */ 
  void write_and_refresh() { 
    write_changes();
  }

  // --- sharding ---

  /**
   * Returns the ID of the shard that owns this vertex
   */
  graph_shard_id_t master_shard() const {
    return master;
  };

  /**
   * returns the number of shards this vertex spans
   */
  size_t get_num_shards() const {
    return mirrors.size() + 1;
  };

  /**
   * returns a vector containing the shard IDs this vertex spans
   */
  std::vector<graph_shard_id_t> get_shard_list() const {
    std::vector<graph_shard_id_t> ret(mirrors.size());
    foreach(const graph_shard_id_t& mirror, mirrors) {
      ret.push_back(mirror);
    }
    ret.push_back(master);
    return ret;
  };

  // --- adjacency ---
  /** gets part of the adjacency list of this vertex belonging on shard shard_id
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
   *  The prefetch behavior is ignored. We always pass the data pointer to the new edge. 
   *
   *  Assuming the shardid  is a local shard.
   */ 
  void get_adj_list(graph_shard_id_t shard_id, 
                            bool prefetch_data,
                            std::vector<graph_edge*>* out_inadj,
                            std::vector<graph_edge*>* out_outadj) {
    database->get_adj_list(vid, shard_id, prefetch_data, out_inadj, out_outadj);
  }
}; // end of class
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
