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
        oarc << inEdges[i].get_src() << inEdges[i].get_id() << inEdges[i].data();
      for (size_t i = 0; i < num_out_edges; i++) 
        oarc << outEdges[i].get_dest() << outEdges[i].get_id() << outEdges[i].data();
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

  /** 
   * Returns a pointer to the graph_row representing the data
   * stored on this edge. Modifications made to the data, are only committed 
   * to the database through a write_* call.
   *
   * \note Note that a pointer to the graph_row is returned. The graph_edge 
   * object retains ownership of the graph_row object. If this edge is freed 
   * (using \ref graph_database::free_edge ),  all pointers to the data 
   * returned by this function are invalidated.
   */
  graph_row* data()  {
    if (edata == NULL)
      refresh();
    return edata;
  };

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
    char* msg = messages.update_edge_request(&len, eid, master, edata);
    std::string reply = graph->update(graph->find_server(master), msg, len); 
    std::string errormsg;
    ASSERT_TRUE(messages.parse_reply(reply, errormsg));

    for (size_t i = 0; i < data()->num_fields(); i++) {
      graph_value* val = data()->get_field(i);
      if (val->get_modified()) {
        val->post_commit_state();
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
    char* msg = messages.update_edge_request(&len, eid, master, edata);
    graph->update_async(graph->find_server(master), msg, len, reply_queue); 
    //TODO: check reply success
    for (size_t i = 0; i < data()->num_fields(); i++) {
      graph_value* val = data()->get_field(i);
      if (val->get_modified()) {
        val->post_commit_state();
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
    oarc << sourceid << targetid << eid << master;
    if (edata == NULL) {
      oarc << false;
    } else {
      oarc << true << *edata;
    }
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

  std::vector<query_result> reply_queue;
};

} // namespace graphlab
#endif
