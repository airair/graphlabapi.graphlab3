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

  // modified_values[i] is set the new value of field i or NULL if field i is not modified.
  // the stored pointer is responsible for free the resources.
  std::vector<graph_value*> modified_values;

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
  graph_vertex_remote() : 
      vid(-1), vdata(NULL), master(-1), graph(NULL) {}

  graph_vertex_remote(graph_client* graph) : 
      vid(-1), vdata(NULL), master(-1), graph(graph) {}

  ~graph_vertex_remote() { 
    if (vdata != NULL) {
      delete vdata;
    }
    for (size_t i = 0; i < modified_values.size(); i++) {
      if (modified_values[i] != NULL) {
        delete modified_values[i];
        modified_values[i] = NULL;
      }
    }
  }

  /**
   * Returns the ID of the vertex
   */
  graph_vid_t get_id() const {
    return vid;
  }

  /**
   * Returns a const pointer to the graph_row representing the data
   * stored on this vertex. Modifications made to the data can only be done
   * through set_field and changes are only committed 
   * to the database through a write_* call.
   */
  const graph_row* immutable_data() const {
    return vdata;
  };

  /**
   * Set the field at fieldpos to new value. 
   * Modifications made to the data, are only committed 
   * to the database through a write_* call.
   */
  inline bool set_field(size_t fieldpos, const graph_value& value) {
    if (fieldpos >= num_fields()) {
      return false;
    }
    if (fieldpos >= modified_values.size()) {
      modified_values.resize(fieldpos+1);
    }
    if (modified_values[fieldpos] == NULL) {
      graph_value* val = new graph_value();
      modified_values[fieldpos] = val;
    }
    // copy over old data
    *modified_values[fieldpos] = *(vdata->get_field(fieldpos)); 
    return modified_values[fieldpos]->set_val(value);
  }

  /**
   * Return a pointer to the graph value at the requested field.
   *
   * \note Instead of pointing to the original value, the pointer points
   * to an ghost copy of the actual field.
   *
   * \note Modification made to the data should be commited through a write_* call.
   */
  inline graph_value* get_field(size_t fieldpos) {
    if (fieldpos >= num_fields()) {
      return NULL;
    }
    if (fieldpos >= modified_values.size()) {
      modified_values.resize(fieldpos+1);
    }
    if (modified_values[fieldpos] == NULL) {
      graph_value* val = new graph_value();
      modified_values[fieldpos] = val;
    }
    // copy over old data
    *modified_values[fieldpos] = *(vdata->get_field(fieldpos)); 
    return modified_values[fieldpos];
  }

  /// Returns number of fields in the data.
  inline size_t num_fields() const {
    return immutable_data()->num_fields();
  }

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
    char* msg = messages.update_vertex_request(&len, vid, master, modified_values, vdata);
    std::string reply = graph->update(graph->find_server(master), msg, len); 
    std::string errormsg;
    ASSERT_TRUE(messages.parse_reply(reply, errormsg));
    for (size_t i = 0; i < modified_values.size(); i++) {
      if (modified_values[i] != NULL) {
        delete modified_values[i];
        modified_values[i] = NULL;
      }
    }
  }

  /**
   * Same as synchronous commit in shared memory.
   */ 
  void write_changes_async() { 
    if (vdata == NULL)
      return;
    QueryMessages messages;

    int len;
    char* msg = messages.update_vertex_request(&len, vid, master, modified_values, vdata);
    graph->update_async(graph->find_server(master), msg, len, reply_queue); 

    //TODO: check reply success
    reply_queue.clear();

    for (size_t i = 0; i < modified_values.size(); i++) {
      if (modified_values[i] != NULL) {
        delete modified_values[i];
        modified_values[i] = NULL;
      }
    }
  }

  /**
   * Request vertex data from the server.
   */ 
  void refresh() { 
    QueryMessages messages;
    int len;
    char* request = messages.vertex_row_request(&len, vid, master);
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
   * Returns the ID of the shard that owns this vertex
   */
  std::vector<graph_shard_id_t> mirror_shards() const {
    return mirrors;
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
    external_save(oarc, this);
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

  static void external_save(oarchive& oarc,
                            const graph_vertex* v) {
    oarc << v->get_id() << v->master_shard() << v->mirror_shards() ;
    if (v->immutable_data() == NULL) {
      oarc << false;
    } else {
      oarc << true << *(v->immutable_data());
    }
  }


  friend class graph_database_server;
  friend class distributed_graph_client;
}; // end of class
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
