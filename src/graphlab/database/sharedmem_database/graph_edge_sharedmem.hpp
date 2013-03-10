#ifndef GRAPHLAB_DATABASE_GRAPH_EDGE_SHAREDMEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_EDGE_SHAREDMEM_HPP
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_edge.hpp>
namespace graphlab {

  class graph_database_sharedmem;

/**
 * \ingroup group_graph_database
 *  An shared memory implementation of <code>graph_edge</code>.
 *  The edge data is directly accessible through pointers. 
 *
 * This object is not thread-safe, and may not copied.
 */
class graph_edge_sharedmem : public graph_edge {
 graph_eid_t eid;
 graph_shard_id_t master;
 graph_database* database;
 public:
  inline graph_edge_sharedmem() : eid(-1), master(-1), database(NULL) {}

  inline graph_edge_sharedmem(graph_eid_t edgeid,
                       graph_shard_id_t master,
                       graph_database* database) :
    eid(edgeid), master(master), database(database) {}

  /**
   * Returns the source ID of this edge
   */
  inline graph_vid_t get_src() const { return database->get_shard(master)->edge(eid).first;} 

  /**
   * Returns the destination ID of this edge
   */
  inline graph_vid_t get_dest() const { return database->get_shard(master)->edge(eid).second;}

  /**
   * Returns the internal id of this edge
   * The id is unique with repect to a shard.
   */
  inline graph_eid_t get_id() const { return eid;};

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
  inline graph_row* data()  {
    return database->get_shard(master)->edge_data(eid);
  };

  inline const graph_row* immutable_data() const {
    return database->get_shard(master)->edge_data(eid);
  }

  // --- synchronization ---


  /**
   * Commits changes made to the data on this vertex synchronously.
   * This resets the modification and delta flags on all values in the 
   * graph_row.
   */ 
  inline void write_changes() {  
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
  inline void write_changes_async() { 
    write_changes();
  }

  /**
   * Fetch the edge pointer from the right shard. 
   */ 
  inline void refresh() { }

  /**
   * Commits the change immediately.
   * Refresh has no effects in shared memory.
   */ 
  inline void write_and_refresh() { 
    write_changes();
  }

 /**
   * Returns the ID of the shard owning this edge
   */
  inline graph_shard_id_t master_shard() const {
    return master;
  };

  friend class graph_database_sharedmem;
};

} // namespace graphlab
#endif
