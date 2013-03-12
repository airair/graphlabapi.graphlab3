#ifndef GRAPHLAB_DATABASE_ERROR_MESSAGES_HPP
#define GRAPHLAB_DATABASE_ERROR_MESSAGES_HPP
#include <string>
#include <vector>
#include <boost/lexical_cast.hpp>
namespace graphlab {
  class ErrorMessages {
   public:
    // "Server is not reachable..."
    inline std::string server_not_reachable(const std::string& server_name) {
      return std::string("Server: " + server_name + " is not reachable.");
    }
    // "Could not find vertex ..."
    inline std::string vertex_not_found(graph_vid_t vid, graph_shard_id_t shardid) {
      return std::string("Could not find vertex id = ")
          + boost::lexical_cast<std::string>(vid)
          + (" on shard : ")
          + boost::lexical_cast<std::string>(shardid);
    }

    // "Could not find shard..."
    inline std::string shard_not_found(graph_shard_id_t shardid) {
      return std::string("Could not find shard id = ")
          + boost::lexical_cast<std::string>(shardid);
    }

    // "Could not find shard..."
    inline std::string edge_not_found(graph_eid_t eid, graph_shard_id_t shardid) {
      return std::string("Could not find edge id = ") 
          + boost::lexical_cast<std::string>(eid)
          + " on shard id = " + boost::lexical_cast<std::string>(shardid);
    }

    // "Fail setting value..."
    inline std::string fail_setting_value(size_t fieldpos) {
      return std::string("Fail setting field ")
          + boost::lexical_cast<std::string>(fieldpos);
    }

    // "Fail adding vertex..." 
    inline std::string fail_adding_vertex(graph_vid_t vid) {
      return std::string("Fail adding vertex ") 
          + boost::lexical_cast<std::string>(vid);
    }

    // "Fail adding edge..."
    inline std::string fail_adding_edge(graph_vid_t src, graph_vid_t dest) {
      return std::string("Fail adding vertex (") 
          + boost::lexical_cast<std::string>(src) + ", " + boost::lexical_cast<std::string>(dest) + ")";
    }

    // "Fail adding vertex mirror..." 
    inline std::string fail_adding_mirror(graph_vid_t vid, graph_shard_id_t master,
                                           graph_shard_id_t mirror) {
      return std::string("Fail adding mirror ") + boost::lexical_cast<std::string>(mirror)
          + "for vertex " + boost::lexical_cast<std::string>(vid);
      + " on master shard " + boost::lexical_cast<std::string>(master);
    }

    // "Unknown query header ... "
    inline std::string unknown_query_header(std::string& header) {
      return std::string("Unknown query header: ") + header; 
    }

    // "Fail remove field, field not found ... "
    inline std::string field_not_found(size_t fieldpos) {
      return std::string("Fail removing field: Field ") + boost::lexical_cast<std::string>(fieldpos) + " not found"; 
    }

    // "Field already exists ... "
    inline std::string field_duplicate(std::string fieldname) {
      return std::string("Fail adding field: Field ") + fieldname + " already exists"; 
    }
  }; // class
}// namespace
#endif
