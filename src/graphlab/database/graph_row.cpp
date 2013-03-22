#include <graphlab/database/graph_row.hpp>
namespace graphlab {
  graph_row::graph_row(const std::vector<graph_field>& fields, bool is_vertex) : _is_vertex(is_vertex) {
        _data.resize(fields.size());
        for (size_t i = 0; i < fields.size(); i++) {
          _data[i].init(fields[i].type);
        }
      }

  void graph_row::add_field(graph_field& field) {
    graph_value v(field.type);
    _data.push_back(v);
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


