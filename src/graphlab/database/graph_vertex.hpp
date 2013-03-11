#ifndef GRAPHLAB_DATABASE_GRAPH_VERTEX_HPP
#define GRAPHLAB_DATABASE_GRAPH_VERTEX_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
namespace graphlab {


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
class graph_vertex {
 public:

  graph_vertex() {}

  inline virtual ~graph_vertex() { }

  /**
   * Returns the ID of the vertex
   */
  virtual graph_vid_t get_id() const = 0;

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
  virtual graph_row* data() = 0;
  
  virtual const graph_row* immutable_data() const = 0;

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
  virtual void write_changes() = 0;

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
  virtual void write_changes_async() = 0;

  /**
   * Synchronously refreshes the local copy of the data from the database, 
   * discarding all changes if any. This call may invalidate all previous
   * graph_row pointers returned by \ref data() . 
   *
   * \note The function should also reset the modification flags, delta_commit 
   * flags and update the _old values for each graph_value in the graph_row.
   */ 
  virtual void refresh() = 0;

  /**
   * Synchronously commits all changes made to the data on this vertex, and
   * refreshes the local copy of the data from the database. Equivalent to a
   * a call to \ref write_changes() followed by \ref refresh() and may be 
   * implemented that way. This call may invalidate all previous
   * graph_row pointers returned by \ref data() . 
   */ 
  virtual void write_and_refresh() = 0;

  // --- sharding ---

  /**
   * Returns the ID of the shard owning this vertex
   */
  virtual graph_shard_id_t master_shard() const = 0;

  /**
   * Returns the IDs of the shards with mirror of this vertex
   */
  virtual std::vector<graph_shard_id_t> mirror_shards() const = 0;


  /**
   * returns the number of shards this vertex spans
   */
  virtual size_t get_num_shards() const = 0;

  /**
   * returns a vector containing the shard IDs this vertex spans (master + mirrors)
   */
  virtual std::vector<graph_shard_id_t> get_shard_list() const = 0;

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
  virtual void get_adj_list(graph_shard_id_t shard_id, bool prefetch_data,
                            std::vector<graph_edge*>* out_inadj,
                            std::vector<graph_edge*>* out_outadj) = 0;



 private:
  // copy constructor deleted. It is not safe to copy this object.
  graph_vertex(const graph_vertex&) { }

  // assignment operator deleted. It is not safe to copy this object.
  graph_vertex& operator=(const graph_vertex&) { return *this; }

  // output the string format to ostream.
  friend std::ostream& operator<<(std::ostream &strm, graph_vertex& v) {
    strm << "vertex: " << v.get_id() << "\t" << "master: " << v.master_shard() << "\n"
         << "span: [";
    const std::vector<graph_shard_id_t>& spans = v.get_shard_list();
    for (size_t i = 0; i < spans.size(); i++) {
        strm << spans[i];
        if (i < spans.size()-1)
          strm << ",";
    }
    strm << "]\n";
    if (v.data() != NULL) {
      strm << "data: " << *(v.data()) << std::endl;
    } else {
      strm  << "data: " << "not available" << std::endl;
    }
    return strm;
  }
};

} // namespace graphlab

#endif
