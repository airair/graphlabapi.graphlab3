#include<graphlab/database/client/cli_parser.hpp>
namespace graphlab {
    bool cli_parser::parse(std::istream& in, parsed_command& out) {
      std::string cmd, target, val;
      in >> cmd >> target;
      getline(in, val);
      boost::algorithm::trim(val);
      commandtype_enum cmd_type;
      targettype_enum target_type;
      if (!try_parse_cmd(cmd, cmd_type)) {
         logstream(LOG_WARNING) << "Unknown command: " << cmd << std::endl; 
         return false;
      }
      if (!try_parse_target(target, target_type)) {
         logstream(LOG_WARNING) << "Unknown target: " << target << std::endl; 
         return false;
      }
      out.cmd_type = cmd_type;
      out.target_type = target_type;

      if (target_type == STRING_TYPE) {
        out.value = target + " " + val;
      } else {
        out.value = val;
      }
      return true;
    }

    bool cli_parser::try_parse_cmd(std::string str, commandtype_enum& cmd) {
      if (str == "get") {
        cmd = GET_TYPE;
      } else if (str == "set") {
        cmd = SET_TYPE;
      } else if (str == "add") {
        cmd = ADD_TYPE;
      }  else if (str == "load") {
        cmd = LOAD_TYPE;
      } else if (str == "batch_get") {
        cmd = BATCH_GET_TYPE;
      } else if (str == "batch_add") {
        cmd = BATCH_ADD_TYPE;
      } else {
        return false;
      }
      return true;
    }

    bool cli_parser::try_parse_target(std::string str, targettype_enum& target) {
      if (str == "vertex") {
        target = VERTEX_TYPE;
      } else if (str == "vertex_adj") {
        target = VERTEX_ADJ_TYPE;
      } else if (str == "vertex_field") {
        target = VERTEX_FIELD_TYPE;
      } else if (str == "edge_field") {
        target = EDGE_FIELD_TYPE;
      } else if (str == "shard") {
        target = SHARD_TYPE;
      } else if (str == "shard_adj") {
        target = SHARD_ADJ_TYPE;
      } else if (str == "graph_info") {
        target = GRAPH_INFO_TYPE;
      } else {
        target = STRING_TYPE;
      }
      return true;
    }
}// end of namespace
