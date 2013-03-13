#include <graphlab/database/graph_row.hpp>
namespace graphlab {

  graph_row::graph_row(std::vector<graph_field>& fields, bool is_vertex) :
      _own_data(true), _nfields(fields.size()), _is_vertex(is_vertex) {

        _data =  new graph_value[fields.size()];
        for (size_t i = 0; i < fields.size(); i++) {
          _data[i].init(fields[i].type);
        }
      }

  void graph_row::add_field(graph_field& field) {
    graph_value* new_data = new graph_value[_nfields+1];
    memcpy(new_data, _data, _nfields*sizeof(graph_value));
    new_data[_nfields].init(field.type);
    delete[] _data;
    _data = new_data;
    ++_nfields;
  }

  void graph_row::remove_field(size_t fieldpos) {
    graph_value* new_data = new graph_value[_nfields-1];
    // copy field 0 to fieldpos-1
    if (fieldpos > 0)
      memcpy(new_data, _data, fieldpos*sizeof(graph_value)); 
    // copy field fieldpos+1 to _nfields
    if (fieldpos < _nfields-1)
      memcpy(new_data+fieldpos, _data+fieldpos+1, (_nfields-1-fieldpos)*sizeof(graph_value));
    delete[] _data;
    _data = new_data;
    --_nfields;
  }

  void graph_row::shallowcopy(graph_row& out_row) {
    memcpy(&out_row, this, sizeof(graph_row));
    out_row._own_data = false;
  }

  void graph_row::deepcopy(graph_row& out_row) {
    out_row._is_vertex = _is_vertex;
    out_row._nfields = _nfields;
    out_row._own_data = true;
    out_row._data = new graph_value[num_fields()];
    for (size_t i = 0; i < num_fields(); i++) {
      _data[i] = out_row._data[i];
    }
  }

  void graph_row::copy_transfer_owner(graph_row& out_row) {
    ASSERT_TRUE(_own_data);
    memcpy(&out_row, this, sizeof(graph_row));
    _own_data = false;
  }

} // namespace graphlab

// out_row._database = _database;
// int graph_row::get_field_pos(const char* fieldname) {
//   int fieldpos = -1;
//   if (is_vertex()) {
//     fieldpos = _database->find_vertex_field(fieldname);
//   } else {
//     fieldpos = _database->find_edge_field(fieldname);
//   }
//   return fieldpos;
// } 

// graph_value* graph_row::get_field(const char* fieldname) {
//   int fieldpos = get_field_pos(fieldname);
//   if (fieldpos < 0) {
//     return NULL;
//   } else {
//     graph_value* ret = get_field(fieldpos);
//     // this cannot possibly be NULL. This means that there is a disagreement
//     // between what the database thinks are the fields, and what the row
//     // thinks are the fields
//     assert(ret != NULL);
//     return ret;
//   }
// }

// std::string graph_row::get_field_metadata(size_t fieldpos) {
//   if (is_vertex()) {
//     const std::vector<graph_field>& fields = _database->get_vertex_fields();
//     if (fieldpos < fields.size()) {
//       return fields[fieldpos].name;
//     } else {
//       return "";
//     }
//   } else { 
//     const std::vector<graph_field>& fields = _database->get_edge_fields();
//     if (fieldpos < fields.size()) {
//       return fields[fieldpos].name;
//     } else {
//       return "";
//     }
//   }
// }


