#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/logger/assertions.hpp>

namespace graphlab {

int graph_row::get_field_pos(const char* fieldname) {
  int fieldpos = -1;
  if (is_vertex()) {
    fieldpos = _database->find_vertex_field(fieldname);
  } else {
    fieldpos = _database->find_edge_field(fieldname);
  }
  return fieldpos;
} 

graph_value* graph_row::get_field(size_t fieldpos) {
  if (fieldpos < num_fields()) return _data + fieldpos;
  else return NULL; 
}

graph_value* graph_row::get_field(const char* fieldname) {
  int fieldpos = get_field_pos(fieldname);
  if (fieldpos < 0) {
    return NULL;
  } else {
    graph_value* ret = get_field(fieldpos);
    // this cannot possibly be NULL. This means that there is a disagreement
    // between what the database thinks are the fields, and what the row
    // thinks are the fields
    assert(ret != NULL);
    return ret;
  }
}

std::string graph_row::get_field_metadata(size_t fieldpos) {
  if (is_vertex()) {
    const std::vector<graph_field>& fields = _database->get_vertex_fields();
    if (fieldpos < fields.size()) {
      return fields[fieldpos].name;
    } else {
      return "";
    }
  } else { 
    const std::vector<graph_field>& fields = _database->get_edge_fields();
    if (fieldpos < fields.size()) {
      return fields[fieldpos].name;
    } else {
      return "";
    }
  }
}

void graph_row::shallowcopy(graph_row& out_row) {
  memcpy(&out_row, this, sizeof(graph_row));
  out_row._own_data = false;
}

void graph_row::deepcopy(graph_row& out_row) {
  out_row._database = _database;
  out_row._is_vertex = _is_vertex;
  out_row._nfields = _nfields;
  out_row._own_data = true;
  out_row._data = new graph_value[num_fields()];
  for (size_t i = 0; i < num_fields(); i++) {
    _data[i].deepcopy(out_row._data[i]);
  }
}

void graph_row::copy_transfer_owner(graph_row& out_row) {
  ASSERT_TRUE(_own_data);
  memcpy(&out_row, this, sizeof(graph_row));
  _own_data = false;
}

} // namespace graphlab
