#ifndef GRAPHLAB_DATABASE_GRAPH_ROW_HPP
#define GRAPHLAB_DATABASE_GRAPH_ROW_HPP
#include <vector>
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
 *  This struct is intentionally make fully public to allows the graph_row
 *  type to be used natively in the database implementations easily.
 */
class graph_row {
 public:
  /// An array of all the values in this row
  std::vector<graph_value> _data;

  /// If true, this represents a vertex; if false, this represents an edge.
  bool _is_vertex;

  /// Empty constructor 
  inline graph_row() : _is_vertex(true) { }
  
  /// Given fields metadata, creates a row with NULL values in the given fields.
  graph_row(const std::vector<graph_field>& fields, bool is_vertex); 
  
  /// Destructor. Frees the values if <code>_own_data</code> is true.
  inline ~graph_row() { }

  /// add a new field into row with NULL value.
  void add_field(graph_field& field);

  /// Returns the number of fields on this row
  inline size_t num_fields() const {
    return _data.size(); 
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
    if (num_fields() == 0) {
      return true;
    }

    for (size_t i = 0; i < num_fields(); i++) {
      if (!_data[i].is_null())
        return false;
    }

    return true;
  }

  /** 
   * Returns a pointer to the value of a particular position in the row.
   * Returns a pointer to the value. Returns NULL if the position is invalid.
   */
  inline graph_value* get_field(size_t fieldpos) {
    if (fieldpos < num_fields()) 
      return (&_data[0] + fieldpos);
    else
      return NULL; 
  }

  /** 
   * Const version of get_field. 
   */
  inline const graph_value* get_field(size_t fieldpos) const {
    if (fieldpos < num_fields()) 
      return (&_data[0] + fieldpos);
    else
      return NULL; 
  }

  /**
   * Serialization interface. Save the values and associated state into oarchive.
   */
  void save (oarchive& oarc) const {
    oarc << _is_vertex << _data;
  }

  /**
   * Serialization interface. Load the values and associated state from iarchive.
   * The graph_row owns the data loaded from the iarchive. 
   */
  void load (iarchive& iarc) {
    iarc >> _is_vertex >> _data;
  }

 private:
  
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
};
} // namespace graphlab 
#endif
