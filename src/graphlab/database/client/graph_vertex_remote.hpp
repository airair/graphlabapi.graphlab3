#ifndef GRAPHLAB_DATABASE_GRAPH_VERTEX_REMOTE_HPP
#define GRAPHLAB_DATABASE_GRAPH_VERTEX_REMOTE_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/client/graph_client.hpp>
#include <graphlab/database/client/graph_edge_remote.hpp>
#include <graphlab/database/query_messages.hpp>
#include <fault/query_object_client.hpp>
#include <boost/unordered_set.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

class distributed_graph_client;
/**
 * \ingroup group_graph_database
 *  An implementation of <code>graph_vertex</code> for the distributed_graph client.
 * This class is responsible for the free of the data pointers. 
 *
 * This object is not thread-safe, and may not copied.
 */
class graph_vertex_remote: public graph_vertex {
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
  graph_client* graph;


  typedef libfault::query_object_client::query_result query_result;

  std::vector<query_result> reply_queue;

 public:
  /**
   * Creates a graph vertex object 
   */
  graph_vertex_remote(graph_client* graph) : 
      vid(-1), vdata(NULL), master(-1), graph(graph) {}
  
  ~graph_vertex_remote() { 
    if (vdata != NULL) {
      delete vdata;
    }
  }

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
   */ 
  void write_changes() {  
    if (vdata == NULL)
      return;
    QueryMessages messages;
    int len;
    char* msg = messages.update_vertex_request(&len, vid, vdata);
    std::string reply = graph->update(graph->find_server(master), msg, len); 
    std::string errormsg;
    ASSERT_TRUE(messages.parse_reply(reply, errormsg));
  }

  /**
   * Same as synchronous commit in shared memory.
   */ 
  void write_changes_async() { 
    if (vdata == NULL)
      return;
    QueryMessages messages;
    int len;
    char* msg = messages.update_vertex_request(&len, vid, vdata);
    graph->update_async(graph->find_server(master), msg, len, reply_queue); 
  }

  /**
   * Request vertex data from the server.
   */ 
  void refresh() { 
    QueryMessages messages;
    int len;
    char* request = messages.vertex_row_request(&len, vid);
    std::string reply = graph->query(graph->find_server(master), request, len);

    if (vdata ==  NULL)
      vdata = new graph_row(); 
    std::string errormsg;

    bool success = messages.parse_reply(reply, *vdata, errormsg);
    ASSERT_TRUE(success);
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

    graph_edge_remote::vertex_adjacency_record record(graph);

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
      if (vdata == NULL)
        vdata = new graph_row();
      iarc >> *vdata;
    }
  }

  friend class graph_database_server;
}; // end of class
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
