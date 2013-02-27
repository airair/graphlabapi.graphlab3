#ifndef GRAPHLAB_DATABASE_GRAPH_FIELD_HPP
#define GRAPHLAB_DATABASE_GRAPH_FIELD_HPP
#include <string>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>

namespace graphlab {

/**
 * \ingroup group_graph_database
 * Describes the meta-data for a field stored on vertices or on edges.
 * Provides information such as the name of the field, the datatype
 * as well as several other properties.
 */
struct graph_field {
  std::string name;
  bool is_indexed;
  graph_datatypes_enum type;
  // not used yet... TODO: check max_data_length for graph_value.
  size_t max_data_length; 

  graph_field(std::string name, graph_datatypes_enum type) :
     name(name), is_indexed(false), type(type), max_data_length(0) {} 

  void save(oarchive &oarc) const {
    oarc << name << is_indexed << type << max_data_length;
  }
  void load(iarchive &iarc) {
    iarc >> name >> is_indexed >> type >> max_data_length;
  }

  graph_field() {}
};

} // namespace graphlab
#endif 
