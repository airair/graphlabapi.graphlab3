#ifndef GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_EDGE_HPP
#define GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_EDGE_HPP
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/query_messages.hpp>
#include <graphlab/database/distributed_graph/idistributed_graph.hpp>
namespace graphlab {

/**
 * \ingroup group_graph_database
 *  An shared memory implementation of <code>graph_edge</code>.
 *  The edge data is directly accessible through pointers. 
 *
 * This object is not thread-safe, and may not copied.
 */
class distributed_graph_edge : public graph_edge {

 graph_vid_t sourceid;

 graph_vid_t targetid;

 graph_eid_t eid;

 graph_row* edata;

 graph_shard_id_t master;

 idistributed_graph* graph;

 public:
  struct vertex_adjacency_record {
    graph_vid_t vid; 
    graph_shard_id_t shardid;
    distributed_graph_edge* inEdges;
    distributed_graph_edge* outEdges;
    idistributed_graph* graph;
    size_t num_in_edges, num_out_edges;

    vertex_adjacency_record(idistributed_graph* graph) : graph(graph) {}

    void save (oarchive& oarc) const{
      oarc << vid << shardid << num_in_edges << num_out_edges;
      for (size_t i = 0; i < num_in_edges; i++)
        oarc << inEdges[i].get_src() << inEdges[i].get_id() << inEdges[i].data();
      for (size_t i = 0; i < num_out_edges; i++) 
        oarc << outEdges[i].get_dest() << outEdges[i].get_id() << outEdges[i].data();
    }

    void load (iarchive& iarc) {
      iarc >> vid >> shardid >> num_in_edges >> num_out_edges;
      inEdges = new distributed_graph_edge[num_in_edges];
      outEdges = new distributed_graph_edge[num_out_edges];
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


 public:
 distributed_graph_edge() :
     sourceid(-1), targetid(-1), eid(-1), edata(NULL),
     master(-1), graph(NULL) {}

 ~distributed_graph_edge () {
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
   * Commits changes made to the data on this vertex synchronously.
   * This resets the modification and delta flags on all values in the 
   * graph_row.
   *
   * TODO: check delta commit.
   */ 
  void write_changes() {  
    if (edata == NULL)
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
   * No effects in shared memory.
   */ 
  void refresh() { 
    ASSERT_TRUE(false);
  }

  /**
   * Commits the change immediately.
   * Refresh has no effects in shared memory.
   */ 
  void write_and_refresh() { 
    write_changes();
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
      edata = new graph_row();
      iarc >> *edata;
    }
  }
};

} // namespace graphlab
#endif
