#ifndef GRAPHLAB_DATABASE_GRAPHDB_CONFIG_HPP
#define GRAPHLAB_DATABASE_GRAPHDB_CONFIG_HPP
#include <graphlab/database/graph_field.hpp>
#include <graphlab/logger/logger.hpp>

#include <fstream>
#include <vector>

namespace graphlab {
  class graphdb_config {
   public:
    graphdb_config(std::string fname) { 
      if (!parse(fname)) {
        logstream(LOG_FATAL) << "Abort: Fail parsing graphdb configure file." << std::endl; 
      } 
    }

    size_t get_nshards() { return nshards; }

    const std::vector<graph_field> get_vertex_fields() {
      return vertex_fields;
    }

    const std::vector<graph_field> get_edge_fields() {
      return edge_fields;
    } 

    std::vector<std::string> get_zkhosts() {
      return zk_hosts;
    }

    std::string get_zkprefix() {
      return zk_prefix;
    }

    std::vector<std::string> get_serveraddrs() {
      return server_addrs;
    }

   private:

    bool parse(std::string fname);

    bool parse_zkinfo(std::ifstream& in);

    // bool parse_fields(std::ifstream& in, std::vector<graph_field>& fields);


   private:

    size_t nshards;

    std::vector<std::string> zk_hosts;

    std::vector<std::string> server_addrs;

    std::string zk_prefix;

    std::vector<graph_field> vertex_fields;

    std::vector<graph_field> edge_fields;
  };
}
#endif
