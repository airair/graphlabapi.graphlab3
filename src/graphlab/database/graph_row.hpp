#ifndef GRAPHLAB_DATABASE_GRAPH_ROW_HPP
#define GRAPHLAB_DATABASE_GRAPH_ROW_HPP
#include <vector>
#include <cassert>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_value.hpp>
#include <graphlab/logger/assertions.hpp>

namespace graphlab {

// forward declaration of the graph database
class graph_database;

/**
 * \ingroup group_graph_database
 * This class stores the complete row of data for a vertex/edge.
 * It allows query for individual entries (columns) in the row,
 * as well as various meta-data.
 *
 * This object is not thread-safe, and may not copied.
 *
 * \note
 *  This struct is intentionally make fully public to allows the graph_row
 *  type to be used natively in the database implementations easily.
 *
 */
class graph_row {
 public:
  /// A pointer to the parent database
  graph_database* _database;
  
  /// An array of all the values on this row
  graph_value* _data;

  /// If true, this row is responsible for data allocation. 
  bool _own_data;

  /// Number of fields in the row.
  size_t _nfields;

  graph_row() {
    _database= NULL;
    _data = NULL;
    _nfields = 0;
    _own_data = false;
  }

  // Create a row with all NULL values in the given fields.
  graph_row(graph_database* database, std::vector<graph_field>& fields) :
    _database(database), _own_data(true), _nfields(fields.size()) {
    _data =  new graph_value[fields.size()];
    for (size_t i = 0; i < fields.size(); i++) {
      graph_value& val = _data[i];
      val._type = fields[i].type;
      if (val._type == STRING_TYPE || val._type == BLOB_TYPE) {
        val._len=0;
      } else if (val._type == DOUBLE_TYPE) {
        val._len=sizeof(graph_double_t);
      } else {
        val._len=sizeof(graph_int_t);
      }
    }
  }

  ~graph_row() {
    if (_own_data) {
      delete[] _data;
    }
  }
  
  /// If true, this represents a vertex; if false, this represents an edge.
  bool _is_vertex;


  /// Returns the number of fields on this row
  inline size_t num_fields() const {
    return _nfields; 
  }

  /** 
   * Returns true if this row represents a vertex. false otherwise
   */
  inline bool is_vertex() const {
    return _is_vertex;
  }

  /** 
   * Returns true if this row represents a vertex. false otherwise
   */
  inline bool is_edge() const {
    return !_is_vertex;
  }

  /**
   * Returns the position of a particular field name.
   * Returns a value >= 0 on success, and -1 on failure.
   */
  int get_field_pos(const char* fieldname);

  /** 
   * Returns a pointer to the value of a particular position in the row.
   * Returns a pointer to the value. Returns NULL if the position is invalid.
   */
  graph_value* get_field(size_t fieldpos);

  /** 
   * Returns a pointer to the value of a particular field.
   * Returns a pointer to the value. Returns NULL if the name is invalid.
   */
  graph_value* get_field(const char* fieldname);

  /** 
   * Returns the name of a field from its position. 
   * Returns the field name on success and an empty string on failure. 
   */
  std::string get_field_metadata(size_t fieldpos);


 private:
  // copy constructor deleted. It is not safe to copy this object.
  graph_row(const graph_row&) { }

  // assignment operator deleted. It is not safe to copy this object.
  graph_row& operator=(const graph_row&) { return *this; }

  /**
   * Makes a shallow copy of this row into out_row and keep the data ownership. 
   */
  void shallowcopy(graph_row& out_row);

  /**
   * Makes a deep copy of this row into out_row. Ignore all fields in out_row.
   */
  void deepcopy (graph_row& out_row);

  /**
   * Makes a copy of this row into out_row and transfer the data ownership. 
   */
  void copy_transfer_owner(graph_row& out_row);

  friend class graph_database;
  friend class graph_database_sharedmem;
  friend class graph_shard_impl;
};

} // namespace graphlab 
#endif
