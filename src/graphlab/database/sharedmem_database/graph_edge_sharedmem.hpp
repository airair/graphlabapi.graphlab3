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

 // modified_values[i] is set the new value of field i or NULL if field i is not modified.
 // the stored pointer is responsible for free the resources.
 std::vector<graph_value*> modified_values;

 public:
  inline graph_edge_sharedmem() : eid(-1), master(-1), database(NULL) {}

  inline graph_edge_sharedmem(graph_eid_t edgeid,
                       graph_shard_id_t master,
                       graph_database* database) :
    eid(edgeid), master(master), database(database) {
      modified_values.resize(num_fields());
    }

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

  inline const graph_row* immutable_data() const {
    return database->get_shard(master)->edge_data(eid);
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
    *modified_values[fieldpos] = *(immutable_data()->get_field(fieldpos)); 
    return modified_values[fieldpos]->set_val(value);
  }

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
    *modified_values[fieldpos] = *(immutable_data()->get_field(fieldpos)); 
    return modified_values[fieldpos];
  }



  inline size_t num_fields() const {
    return immutable_data()->num_fields();
  }

  // --- synchronization ---
  /**
   * Commits changes made to the data on this vertex synchronously.
   * This resets the modification and delta flags on all values in the 
   * graph_row.
   */ 
  inline void write_changes() {  
    graph_row* oldvalues = data();
    for (size_t i = 0; i < modified_values.size(); i++) {
      if (modified_values[i] != NULL) {
        *(oldvalues->get_field(i)) = *modified_values[i];
        delete modified_values[i];
        modified_values[i] = NULL;
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

 private:
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

  friend class graph_database_sharedmem;
};

} // namespace graphlab
#endif
