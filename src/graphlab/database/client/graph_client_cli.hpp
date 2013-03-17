#ifndef GRAPH_DATABASE_GRAPH_CLIENT_CLI_HPP
#define GRAPH_DATABASE_GRAPH_CLIENT_CLI_HPP
#include <graphlab/database/client/cli_parser.hpp>
#include <graphlab/database/client/graph_client.hpp>
#include <graphlab/database/error_messages.hpp>

namespace graphlab {
  class graph_client_cli {
    typedef cli_parser::parsed_command parsed_command; 
    typedef cli_parser::commandtype_enum command_type; 
    typedef cli_parser::targettype_enum target_type; 

    cli_parser parser;
    graph_client* graph;

    ErrorMessages error_messages;

   public:
    graph_client_cli(graph_client* graph) : graph(graph) { }

    void start() {
      parsed_command cmd;
      bool terminate = false;
      while (!terminate) {
        if (!parser.parse(std::cin, cmd)) {
          continue;
        } else {
          switch(cmd.cmd_type) {
           case cli_parser::GET_TYPE: process_get(cmd); break;
           case cli_parser::SET_TYPE: process_set(cmd); break;
           case cli_parser::ADD_TYPE: process_add(cmd); break;
           case cli_parser::BATCH_ADD_TYPE: process_batch_add(cmd); break;
           case cli_parser::BATCH_GET_TYPE: process_batch_get(cmd); break;
           case cli_parser::LOAD_TYPE: process_load(cmd); break;
           case cli_parser::COMPUTE_TYPE: process_compute(cmd); break;
           case cli_parser::RESET_TYPE: process_reset(cmd); break;
           default:  break;
          }
        }
      }
      std::cout << "graph client cli quit" << std::endl;
    }

    void process_get(parsed_command& cmd);
    void process_add(parsed_command& cmd);
    void process_set(parsed_command& cmd);
    void process_load(parsed_command& cmd);
    void process_batch_get(parsed_command& cmd);
    void process_batch_add(parsed_command& cmd);
    void process_compute(parsed_command& cmd);
    void process_reset(parsed_command& cmd);

    void get_graph_info();
    void get_vertex(graph_vid_t vid);
    void get_shard(graph_shard_id_t shardid);
    void get_vertex_adj(graph_vid_t vid, graph_shard_id_t shardid);
    void get_shard_adj(graph_shard_id_t from, std::vector<graph_vid_t>& vids);
    void set_vertex(graph_vid_t vid, size_t fieldpos, std::string& val); 
    void batch_get_vertices(std::vector<graph_vid_t>& vids);
    void batch_get_vertex_adj_to_shard(graph_shard_id_t shard_from, graph_shard_id_t shard_to);

   private:
    inline void error_unsupported_command() {
      logstream(LOG_WARNING) << "Command is not supported." << std::endl;
    }
  };
} // end of namespace
#endif
