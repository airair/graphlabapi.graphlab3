#ifndef GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_VERTES_HPP
#define GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_VERTES_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <boost/unordered_set.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

class distributed_graph;
/**
 * \ingroup group_graph_database
 *  An shared memory implementation of <code>graph_vertex</code>.
 *  The vertex data is directly accessible through pointers. 
 *  Adjacency information is accessible through <code>edge_index</code>
 *  object passed from the <code>graph_database_sharedmem</code>.
 *
 * This object is not thread-safe, and may not copied.
 */
class distributed_graph_vertex: public graph_vertex {
 private:

  // Id of the vertex.
  graph_vid_t vid;

  // A cache of the vertex data.
  graph_row* vdata;

  // Pointer to the distributed graph.
  distributed_graph* graph;

 public:
  /**
   * Creates a graph vertex object 
   */
  distributed_graph_vertex(graph_vid_t vid,
                           graph_row* data,
                           distributed_graph* graph) : 
      vid(vid), vdata(data), graph(graph) {}

  /**
   * Returns the ID of the vertex
   */
  graph_vid_t get_id() {
    return vid;
  }

  /**
   * Returns a pointer to the graph_row representing the data
   * stored on this vertex. Modifications made to the data, are only committed 
   * to the database through a write_* call.
   */
  graph_row* data() {
    if (data == NULL)
      refresh();
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
    if (data == NULL)
      return;

    // NOT IMPLEMENTED 
    ASSERT_TRUE(false);
  }

  /**
   * Same as synchronous commit in shared memory.
   */ 
  void write_changes_async() { 
    write_changes();
  }

  /**
   * Request vertex data from the server.
   */ 
  void refresh() { 
  }

  /**
   * Commits the change immediately.
   */ 
  void write_and_refresh() { 
    write_changes();
    refresh();
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
   *  The prefetch behavior is ignored. We always pass the data pointer to the new edge. 
   */ 
  void get_adj_list(graph_shard_id_t shard_id, 
                            bool prefetch_data,
                            std::vector<graph_edge*>* out_inadj,
                            std::vector<graph_edge*>* out_outadj) {
    std::vector<size_t> index_in;
    std::vector<size_t> index_out;
    bool getIn = out_inadj!=NULL;
    bool getOut = out_outadj!=NULL;
    edge_index[shard_id]->get_edge_index(index_in, index_out, getIn, getOut, vid);

    foreach(size_t& idx, index_in) {  
      std::pair<graph_vid_t, graph_vid_t> pair = database->get_shard(shard_id)->edge(idx);
      graph_row* row  = database->get_shard(shard_id)->edge_data(idx);
      out_inadj->push_back(new graph_edge_sharedmem(pair.first, pair.second, idx, row, shard_id, database)); 
    }

    foreach(size_t& idx, index_out) {  
      std::pair<graph_vid_t, graph_vid_t> pair = database->get_shard(shard_id)->edge(idx);
      graph_row* row = database->get_shard(shard_id)->edge_data(idx);
      out_outadj->push_back(new graph_edge_sharedmem(pair.first, pair.second, idx, row, shard_id, database)); 
    }
  }
}; // end of class
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
