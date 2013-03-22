#include <graphlab/database/query_message.hpp>
namespace graphlab {  
  const char* QueryMessage::qm_cmd_type_str[6] = {
    "get", "set", "add", "batch_get", "batch_set", "batch_add"
  };

  const char* QueryMessage::qm_obj_type_str[11] = {
    "vertex", "edge", "vertex_adj", "vertex_mirror", "shard",
    "num_vertices", "num_edges", "edge_size", "vertex_field", "edge_field", "undefined"
  };

  QueryMessage::QueryMessage(header h) : h(h) {
    oarc << h.cmd << h.obj;
  }

  QueryMessage::QueryMessage(qm_cmd_type cmd, qm_obj_type obj) : h(cmd, obj) {
    oarc << cmd << obj;
  }

  QueryMessage::QueryMessage(char* msg, size_t len) { 
    iarc = new iarchive(msg, len);
    *iarc >> h.cmd >> h.obj;
  }

  QueryMessage::~QueryMessage() {
    if (iarc != NULL) {
      delete iarc;
    }
  }
} // end of namespace
