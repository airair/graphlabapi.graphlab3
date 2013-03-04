#ifndef GRAPHLAB_DATABASE_GRAPH_ROW_HPP
#define GRAPHLAB_DATABASE_GRAPH_ROW_HPP
#include <vector>
#include <cassert>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_value.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
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
 * \note 
 * This object may or may not be responsible of the data. If <code>_own_data</code>
 * is set, then it obtains the ownership of the data, and will free the data in its deconstructor.
 * graph_row stored in the <code>graph_shard</code> has the ownership of the data.
 *
 * This object is not thread-safe, and may not copied.
 *
 * \note
 *  This struct is intentionally make fully public to allows the graph_row
 *  type to be used natively in the database implementations easily.
 */
class graph_row {
 public:
  /// An array of all the values on this row
  graph_value* _data;

  /// If true, this row is responsible for data allocation. 
  bool _own_data;

  /// Number of fields in the row.
  size_t _nfields;

  /// If true, this represents a vertex; if false, this represents an edge.
  bool _is_vertex;

  /// Empty constructor 
  graph_row() : _data(NULL), _own_data(false), _nfields(0), _is_vertex(false) { }

  /// Given fields metadata, creates a row with NULL values in the given fields.
  graph_row(std::vector<graph_field>& fields, bool is_vertex) :
    _own_data(true), _nfields(fields.size()), _is_vertex(is_vertex) {
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

  /// Destructor. Frees the values if <code>_own_data</code> is true.
  ~graph_row() {
    if (_own_data) {
      delete[] _data;
    }
  }
  
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
   * Returns true if all entries in the row is NULl value. false otherwise
   */
  inline bool is_null() const {
    for (size_t i = 0; i < num_fields(); i++) {
      if (!_data->is_null())
        return false;
    }
    return true;
  }

  // /**
  //  * Returns the position of a particular field name.
  //  * Returns a value >= 0 on success, and -1 on failure.
  //  */
  // int get_field_pos(const char* fieldname);

  /** 
   * Returns a pointer to the value of a particular position in the row.
   * Returns a pointer to the value. Returns NULL if the position is invalid.
   */
  graph_value* get_field(size_t fieldpos);


  /**
   * Serialization interface. Save the values and associated state into oarchive.
   */
  void save (oarchive& oarc) const {
    oarc << _is_vertex << _nfields;
    for (size_t i = 0; i < _nfields; i++) {
      oarc << _data[i];
    }
  }

  /**
   * Serialization interface. Load the values and associated state from iarchive.
   * The graph_row owns the data loaded from the iarchive. 
   */
  void load (iarchive& iarc) {
    iarc >> _is_vertex >> _nfields;
    _own_data = true;
    if (_data == NULL) 
      _data = new graph_value[_nfields];
    for (size_t i = 0; i < _nfields; i++) {
      iarc >> _data[i];
    }
  }

 private:
  // copy constructor deleted. It is not safe to copy this object.
  graph_row(const graph_row&) { }

  // assignment operator deleted. It is not safe to copy this object.
  graph_row& operator=(const graph_row&) { return *this; }

  /**
   * Makes a shallow copy of this row into out_row and retains the data ownership. 
   */
  void shallowcopy(graph_row& out_row);

  /**
   * Makes a deep copy of this row into out_row, which owns the copied data.
   */
  void deepcopy (graph_row& out_row);

  /**
   * Makes a copy of this row into out_row and transfer the data ownership to out_row. 
   */
  void copy_transfer_owner(graph_row& out_row);


  /**
   * Output the string format to ostream.
   */
  friend std::ostream& operator<<(std::ostream &strm, const graph_row& row) {
    if (row._is_vertex) {
      strm << "[v] {";
    }  else {
      strm << "[e] {";
    }
    for (size_t i = 0; i < row.num_fields(); i++) {
      strm  << row._data[i];
      if (i < row.num_fields()-1)
        strm << "\t";
    } 
    strm << "}";
    return strm;
  }

  friend class graph_database;
  friend class graph_database_sharedmem;
  friend struct graph_shard_impl;
};
} // namespace graphlab 
#endif
