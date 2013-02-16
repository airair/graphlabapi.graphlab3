#ifndef GRAPHLAB_DATABASE_GRAPH_EDGE_SHAREDMEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_EDGE_SHAREDMEM_HPP
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
namespace graphlab {

/**
 * \ingroup group_graph_database
 *  An shared memory implementation of <code>graph_edge</code>.
 *  The edge data is directly accessible through pointers. 
 *
 * This object is not thread-safe, and may not copied.
 */
class graph_edge_sharedmem : public graph_edge {
 graph_vid_t sourceid;
 graph_vid_t targetid;
 graph_row* edata;
 graph_shard_id_t master;
 graph_database* database;
 public:
  graph_edge_sharedmem(const graph_vid_t& sourceid,
                       const graph_vid_t& targetid,
                       graph_row* data,
                       graph_shard_id_t master,
                       graph_database* database) :
  sourceid(sourceid), targetid(targetid), edata(data),
    master(master), database(database) {}

  /**
   * Returns the source ID of this edge
   */
  graph_vid_t get_src() { return sourceid; } 

  /**
   * Returns the destination ID of this edge
   */
  graph_vid_t get_dest() { return targetid; }

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
    for (size_t i = 0; i < edata->num_fields(); i++) {
      graph_value* val = edata->get_field(i);
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
   * No effects in shared memory.
   */ 
  void refresh() { }

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
  graph_shard_id_t master_shard() {
    return master;
  };
};

} // namespace graphlab
#endif
