#ifndef GRAPHLAB_DATABASE_GRAPH_VERTEX_SHARED_MEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_VERTEX_SHARED_MEM_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/sharedmem_database/graph_edge_sharedmem.hpp>
#include <graphlab/database/sharedmem_database/graph_vertex_index.hpp>
#include <boost/unordered_set.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

class graph_database_sharedmem;
/**
 * \ingroup group_graph_database
 *  An abstract interface for a vertex of graph.
 *  The interface provides (locally cached) access to the data on the vertex,
 *  and provides control of synchronous and asynchronous modifications to the
 *  vertex. The interface also provides access to adjacency information.
 *
 * This object is not thread-safe, and may not copied.
 *
 * \note For implementors: The interface is designed so that the graph_vertex
 * the graph_vertex can be "lazy" in that it acquires the data on the vertex 
 * (the graph_row object) or the adjacency information (the graph_edge_list), 
 * only when requested.
 *
 * \note This class retains ownership of the pointer returned by data(). 
 * However, this class does not retain ownership of the pointers
 * returned by the get_adj_list() function. This is intentional since it will
 * permit the use of different get_adj_list functions which perform edge
 * subset queries in the future.
 */
class graph_vertex_sharedmem : public graph_vertex {
 private:

  graph_vid_t vid;

  // Cache of the vertex data. 
  graph_row* cache;

  graph_shard_id_t master;

  boost::unordered_set<graph_shard_id_t> mirrors;
  
  // Index of the edges
  std::vector<graph_edge_index>* edge_index;

  graph_database* database;
 public:
  /**
   * Create a graph vertex object 
   */
  graph_vertex_sharedmem(graph_vid_t vid,
                         graph_row* data,
                         graph_shard_id_t master,
                         const boost::unordered_set<graph_shard_id_t> mirrors,
                         std::vector<graph_edge_index>* eindex,
                         graph_database* db) : 
      vid(vid), cache(data), master(master), mirrors(mirrors),
      edge_index(eindex), database(db) {}

  /**
   * Returns the ID of the vertex
   */
  graph_vid_t get_id() {
    return vid;
  }

  /** Returns a pointer to the graph_row representing the data
   * stored on this vertex. Modifications made to the data, are only committed 
   * to the database through a write_* call.
   *
   * \note Note that a pointer to the graph_row is returned. The graph_vertex 
   * object retains ownership of the graph_row object. If this vertex is freed 
   * (using \ref graph_database::free_vertex ),  all pointers to the data 
   * returned by this function are invalidated.
   *
   * \note On the first call to data(), or all calls to *_refresh(), the 
   * graph_vertex performs a synchronous read of the entire row from the
   * database, and caches it. Repeated calls to data() should always return
   * the same graph_row pointer.
   */
  graph_row* data() {
    return cache;
  };

  // --- synchronization ---

  /**
   * Commits changes made to the data on this vertex synchronously.
   * This resets the modification and delta flags on all values in the 
   * graph_row.
   *
   * \note Only values which have been modified should be sent 
   * (see \ref graph_value) and delta changes should be respected.
   * The function should also reset the modification flags, delta_commit flags
   * and update the _old values for each modified graph_value in the 
   * graph_row.
   */ 
  void write_changes() {  
    for (size_t i = 0; i < cache->num_fields(); i++) {
      graph_value* val = cache->get_field(i);
      if (val->get_modified()) {
        val->post_commit_state();
      }
    }
  }

  /**
   * Commits changes made to the data on this vertex asynchronously.
   * This resets the modification and delta flags on all values in the 
   * graph_row.
   *
   * \note There are no guarantees as to when these modifications will be 
   * commited. Just that it will be committed eventually. The graph database
   * may buffer these modifications.
   *
   * \note Only values which have been modified should be sent 
   * (see \ref graph_value) and delta changes should be respected.
   * The function should also reset the modification flags, delta_commit flags
   * and update the _old values for each modified graph_value in the 
   * graph_row.
   */ 
  void write_changes_async() { 
    write_changes();
  }

  /**
   * Synchronously refreshes the local copy of the data from the database, 
   * discarding all changes if any. This call may invalidate all previous
   * graph_row pointers returned by \ref data() . 
   *
   * \note The function should also reset the modification flags, delta_commit 
   * flags and update the _old values for each graph_value in the graph_row.
   */ 
  void refresh() { }

  /**
   * Synchronously commits all changes made to the data on this vertex, and
   * refreshes the local copy of the data from the database. Equivalent to a
   * a call to \ref write_changes() followed by \ref refresh() and may be 
   * implemented that way. This call may invalidate all previous
   * graph_row pointers returned by \ref data() . 
   */ 
  void write_and_refresh() { 
    write_changes();
  }

  // --- sharding ---

  /**
   * Returns the ID of the shard that owns this vertex
   */
  graph_shard_id_t master_shard() {
    return master;
  };

  /**
   * returns the number of shards this vertex spans
   */
  size_t get_num_shards() {
    return mirrors.size() + 1;
  };

  /**
   * returns a vector containing the shard IDs this vertex spans
   */
  std::vector<graph_shard_id_t> get_shard_list() {
    return std::vector<graph_shard_id_t>(mirrors.begin(), mirrors.end());
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
   *  if prefetch_data is set, the data on the retrieved edges will already
   *  be eagerly filled.
   */ 
  void get_adj_list(graph_shard_id_t shard_id, 
                            bool prefetch_data,
                            std::vector<graph_edge*>* out_inadj,
                            std::vector<graph_edge*>* out_outadj) {
    std::vector<size_t> index_in;
    std::vector<size_t> index_out;
    bool getIn = out_inadj!=NULL;
    bool getOut = out_outadj!=NULL;
    edge_index->at(shard_id).get_edge_index(index_in, index_out, getIn, getOut, vid);

    foreach(size_t& idx, index_in) {  
      std::pair<graph_vid_t, graph_vid_t> pair = database->get_shard(shard_id)->edge(idx);
      graph_row* row = NULL;
      if (prefetch_data) {
        row = database->get_shard(shard_id)->edge_data(idx);
      }
      out_inadj->push_back(new graph_edge_sharedmem(pair.first, pair.second, row, shard_id, database)); 
    }

    foreach(size_t& idx, index_out) {  
      std::pair<graph_vid_t, graph_vid_t> pair = database->get_shard(shard_id)->edge(idx);
      graph_row* row = NULL;
      if (prefetch_data) {
        row = database->get_shard(shard_id)->edge_data(idx);
      }
      out_outadj->push_back(new graph_edge_sharedmem(pair.first, pair.second, row, shard_id, database)); 
    }
  }
}; // end of class
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
