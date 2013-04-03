#include <graphlab/database/query_message.hpp>
namespace graphlab {  
  const char* QueryMessage::qm_cmd_type_str[NUM_CMD_TYPE] = {
    "get", "set", "add", "batch_get", "batch_set", "batch_add", "admin"
  };

  const char* QueryMessage::qm_obj_type_str[NUM_OBJ_TYPE] = {
    "vertex", "edge", "vertex_adj", "vertex_mirror", "shard",
    "num_vertices", "num_edges", "vertex_field", "edge_field", "reset", "undefined"
  };

  QueryMessage::QueryMessage(header h) : h(h), iarc(NULL) {
    oarc << h.cmd << h.obj;
  }

  QueryMessage::QueryMessage(qm_cmd_type cmd, qm_obj_type obj) : h(cmd, obj), iarc(NULL) {
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
