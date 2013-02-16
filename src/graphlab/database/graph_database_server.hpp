#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_SERVER_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_SERVER_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/logger/logger.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/serialization/oarchive.hpp>

namespace graphlab {
/**
 * \ingroup group_graph_database
 * An abstract interface for a graph database implementation 
 */
class graph_database_server {
 graph_database* database;
 public:
  
  graph_database_server(graph_database* db) : database(db){ }
  virtual ~graph_database_server() { }

  std::string update(const char* request, size_t len) {
    std::string ret;
    // set vertex data (graph_vid_t vid, size_t fieldpos, void* newdata, size_t len)
    // set edge (shardid, edgeid, size_t fieldpos, void* newdata, size_t len)
    return ret;
  } 

  std::string query(const char* request, size_t len) {
    std::string ret;
    // get vertex fields
    // get edge fields
    // get vertex data (vid, fieldpos)
    // get adjacent edges (vid, bool get_in, bool get_out, bool withdata)
    return  ret;
  }

 private:
  std::string get_vertex_fields() {
    const std::vector<graph_field>& fields = database->get_vertex_fields();
    oarchive oarc;
    oarc << fields;
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }
  std::string get_edge_fields(std::string& str) {
    const std::vector<graph_field>& fields = database->get_edge_fields();
    oarchive oarc;
    oarc << fields;
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }
  std::string get_vertex_data(graph_vid_t vid, size_t fieldpos) {
    graph_vertex* v = database->get_vertex(vid);
    if (v == NULL) {
      std::string errormsg = (std::string("Fail to get vertex id = ") + boost::lexical_cast<std::string>(vid) + std::string(". Vid does not exist."));
      logstream(LOG_WARNING) << errormsg;
      return errormsg;
    }
    else {
      graph_value* val = get_field;
    }
  }
};
} // namespace graphlab
#endif
