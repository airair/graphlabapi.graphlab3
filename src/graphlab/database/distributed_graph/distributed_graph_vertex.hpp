#ifndef GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_VERTEX_HPP
#define GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_VERTEX_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/distributed_graph/idistributed_graph.hpp>
#include <graphlab/database/distributed_graph/distributed_graph_edge.hpp>
#include <graphlab/database/query_messages.hpp>
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

  // Master of the vertex.
  graph_shard_id_t master;

  // Mirrors
  std::vector<graph_shard_id_t> mirrors;

  // Pointer to the distributed graph.
  idistributed_graph* graph;

 public:
  /**
   * Creates a graph vertex object 
   */
  distributed_graph_vertex(idistributed_graph* graph) : 
      vid(-1), vdata(NULL), master(-1), graph(graph) {}
  
  ~distributed_graph_vertex() { }

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
    if (vdata == NULL)
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
    if (vdata == NULL)
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
    ASSERT_TRUE(false);
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
    std::vector<graph_shard_id_t> ret(mirrors);
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
   */ 
  void get_adj_list(graph_shard_id_t shard_id, 
                            bool prefetch_data,
                            std::vector<graph_edge*>* out_inadj,
                            std::vector<graph_edge*>* out_outadj) {
    int msg_len;
    bool getin = !(out_inadj == NULL);
    bool getout = !(out_outadj == NULL);
    QueryMessages messages;
    char* request = messages.vertex_adj_request(&msg_len, vid, shard_id, getin, getout);
    std::string reply = graph->query(graph->find_server(shard_id), request, msg_len);

    distributed_graph_edge::vertex_adjacency_record record(graph);

    std::string errormsg;
    if (messages.parse_reply(reply, record, errormsg)) {
      for (size_t i = 0; i < record.num_in_edges; i++) {
        out_inadj->push_back(&record.inEdges[i]);
      }
      for (size_t i = 0; i < record.num_out_edges; i++) {
        out_outadj->push_back(&record.outEdges[i]);
      }
    }
  }

  void save(oarchive& oarc) const {
    oarc << vid << master << mirrors;
    if (vdata == NULL) {
      oarc << false;
    } else {
      oarc << true << *vdata;
    }
  }

  void load(iarchive& iarc) {
    iarc >> vid >> master >> mirrors;
    bool hasdata = false;
    iarc >> hasdata;
    if (hasdata) {
      vdata = new graph_row();
      iarc >> *vdata;
    }
  }

  friend class graph_database_server;
}; // end of class
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
