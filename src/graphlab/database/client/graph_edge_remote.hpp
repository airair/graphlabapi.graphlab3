#ifndef GRAPHLAB_DATABASE_GRAPH_EDGE_REMOTE_HPP
#define GRAPHLAB_DATABASE_GRAPH_EDGE_REMOTE_HPP
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/query_messages.hpp>
#include <graphlab/database/client/graph_client.hpp>
namespace graphlab {

/**
 * \ingroup group_graph_database
 * An implementation of <code>graph_edge</code> for the distributed_graph client.
 * This class is responsible for the free of the data pointers. 
 *
 * This object is not thread-safe, and may not copied.
 */
class graph_edge_remote : public graph_edge {

 graph_vid_t sourceid;

 graph_vid_t targetid;

 graph_eid_t eid;

 graph_row* edata;

 graph_shard_id_t master;

 // modified_values[i] is set the new value of field i or NULL if field i is not modified.
 // the stored pointer is responsible for free the resources.
 std::vector<graph_value*> modified_values;


 graph_client* graph;

 public:
 /**
  * Struct holds the adjacency list information for the  
  * get_adj_list protocol defined in QueryMessages.
  */
  struct vertex_adjacency_record {
    graph_vid_t vid; 
    graph_shard_id_t shardid;
    graph_edge_remote* inEdges;
    graph_edge_remote* outEdges;
    graph_client* graph;
    size_t num_in_edges, num_out_edges;

    vertex_adjacency_record(graph_client* graph) : vid(-1), shardid(-1), 
        inEdges(NULL), outEdges(NULL), graph(graph), num_in_edges(0),
        num_out_edges(0) {}

    void save (oarchive& oarc) const{
      oarc << vid << shardid << num_in_edges << num_out_edges;
      for (size_t i = 0; i < num_in_edges; i++)
        oarc << inEdges[i].get_src() << inEdges[i].get_id() << inEdges[i].immutable_data();
      for (size_t i = 0; i < num_out_edges; i++) 
        oarc << outEdges[i].get_dest() << outEdges[i].get_id() << outEdges[i].immutable_data();
    }

    void load (iarchive& iarc) {
      iarc >> vid >> shardid >> num_in_edges >> num_out_edges;
      ASSERT_TRUE(inEdges == NULL);
      ASSERT_TRUE(outEdges == NULL);
      inEdges = new graph_edge_remote[num_in_edges];
      outEdges = new graph_edge_remote[num_out_edges];
      for (size_t i = 0; i < num_in_edges; i++) {
        graph_vid_t source; 
        graph_eid_t eid;
        graph_row* data = new graph_row();
        iarc >> source >> eid >> *data; 
        inEdges[i].sourceid = source;
        inEdges[i].targetid = vid;
        inEdges[i].master = shardid;
        inEdges[i].eid = eid;
        inEdges[i].edata = data;
        inEdges[i].graph = graph;
      }
        
      for (size_t i = 0; i < num_out_edges; i++) {
        graph_vid_t target; 
        graph_eid_t eid;
        graph_row* data = new graph_row();
        iarc >> target >> eid >> *data; 
        outEdges[i].sourceid = vid;
        outEdges[i].targetid = target;
        outEdges[i].master = shardid;
        outEdges[i].eid = eid;
        outEdges[i].edata = data;
        outEdges[i].graph = graph;
      }
    }
  };

  typedef libfault::query_object_client::query_result query_result;

 public:
 graph_edge_remote() :
     sourceid(-1), targetid(-1), eid(-1), edata(NULL),
     master(-1), graph(NULL) {}

 graph_edge_remote(graph_client* graph) :
     sourceid(-1), targetid(-1), eid(-1), edata(NULL),
     master(-1), graph(graph) {}


 ~graph_edge_remote () {
   if (edata != NULL)
     delete edata;
   for (size_t i = 0; i < modified_values.size(); i++) {
      if (modified_values[i] != NULL) {
        delete modified_values[i];
        modified_values[i] = NULL;
      }
    }
 }

  /**
   * Returns the source ID of this edge
   */
  graph_vid_t get_src() const { return sourceid; } 

  /**
   * Returns the destination ID of this edge
   */
  graph_vid_t get_dest() const { return targetid; }

  /**
   * Returns the internal id of this edge
   * The id is unique with repect to a shard.
   */
  graph_eid_t get_id() const { return eid;};


  const graph_row* immutable_data() const {
    return edata;
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
    *modified_values[fieldpos] = *(edata->get_field(fieldpos)); 
    return modified_values[fieldpos];
  }


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
    *modified_values[fieldpos] = *(edata->get_field(fieldpos)); 
    return modified_values[fieldpos]->set_val(value);
  }

  /// Returns number of fields in the data.
  inline size_t num_fields() const {
    return immutable_data()->num_fields();
  }
  // --- synchronization ---
  /**
   * Commits changes made to the data on this edge synchronously.
   * This resets the modification and delta flags on all values in the 
   * graph_row.
   */ 
  void write_changes() {  
    if (edata == NULL)
      return;
    QueryMessages messages;
    int len;
    char* msg = messages.update_edge_request(&len, eid, master, modified_values, edata);
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
    if (edata == NULL)
      return;

    QueryMessages messages;
    int len;
    char* msg = messages.update_edge_request(&len, eid, master, modified_values, edata);
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
   * Request the latest data from server synchronously.
   */ 
  void refresh() { 
    QueryMessages messages;
    int len;
    char* request = messages.edge_row_request(&len, eid, master);
    std::string reply = graph->query(graph->find_server(master), request, len);

    if (edata ==  NULL)
      edata = new graph_row(); 
    std::string errormsg;

    bool success = messages.parse_reply(reply, *edata, errormsg);
    ASSERT_TRUE(success);
  }

  /**
   * Commits the change immediately.
   * Refresh has no effects in shared memory.
   */ 
  void write_and_refresh() { 
    write_changes();
    refresh();
  }

 /**
   * Returns the ID of the shard owning this edge
   */
  graph_shard_id_t master_shard() const {
    return master;
  };


  void save(oarchive& oarc) const {
    external_save(oarc, this);
  }

  void load(iarchive& iarc) {
    iarc >> sourceid >> targetid >> eid >> master;
    bool hasdata = false;
    iarc >> hasdata;
    if (hasdata) {
      if (edata == NULL)
        edata = new graph_row();
      iarc >> *edata;
    }
  }

  static void external_save(oarchive& oarc,
                            const graph_edge* e) {
    oarc << e->get_src() << e->get_dest() << e->get_id()
         << e->master_shard();
    if (e->immutable_data() == NULL) {
      oarc << false;
    } else {
      oarc << true << *(e->immutable_data());
    }
  }

  std::vector<query_result> reply_queue;
};

} // namespace graphlab
#endif
