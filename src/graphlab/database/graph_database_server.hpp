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
#include <boost/lexical_cast.hpp>

namespace graphlab {
/**
 * \ingroup group_graph_database
 * An abstract interface for a graph database implementation 
 */
class graph_database_server {
 graph_database* database;
 public:
  
  graph_database_server(graph_database* db) : database(db){ 
    ASSERT_TRUE(db != NULL);
  }
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
    // get vertex data (vid, fieldpos)
    // get adjacent edges (vid, bool get_in, bool get_out, bool withdata)
    return  ret;
  }

  std::string get_vertex_fields() {
    const std::vector<graph_field>& fields = database->get_vertex_fields();
    oarchive oarc;
    oarc << true << fields;
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }

  std::string get_edge_fields() {
    const std::vector<graph_field>& fields = database->get_edge_fields();
    oarchive oarc;
    oarc << true << fields;
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }

  /**
   * Returns the serializatin of the entire row corresponding to the query vertex.
   */
  std::string get_vertex_data(graph_vid_t vid) {
    graph_vertex* v = database->get_vertex(vid);
    oarchive oarc;

    if (v == NULL) {
      std::string errormsg = (std::string("Fail to get vertex id = ") + boost::lexical_cast<std::string>(vid) + std::string(". Vid does not exist."));
      logstream(LOG_WARNING) << errormsg;
      oarc << false << errormsg;
    } else {
      graph_row* row= v->data();
      oarc << true << *row;
    }

    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }

  /**
   * Returns the serialization of graph value corresponding to the query vertex and field index. 
   */
  std::string get_vertex_data(graph_vid_t vid, size_t fieldpos) {
    graph_vertex* v = database->get_vertex(vid);
    oarchive oarc;
    if (v == NULL) {
      std::string errormsg = (std::string("Fail to get vertex id = ") + boost::lexical_cast<std::string>(vid) + std::string(". Vid does not exist."));
      logstream(LOG_WARNING) << errormsg;
      oarc << false << errormsg;
    } else {
      graph_value* val = v->data()->get_field(fieldpos);
      oarc << true << *val;
    }
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }

  /**
   * Returns the serialization of the adjacency structure corresponding to the 
   * query vertex and shard id.
   */
  std::string get_vertex_adj(graph_vid_t vid, graph_shard_id_t shardid, bool get_in, bool get_out, bool prefetch_data) {
    graph_vertex* v = database->get_vertex(vid);
    oarchive oarc;
    if (v == NULL) {
      std::string errormsg = (std::string("Fail to get vertex id = ") + boost::lexical_cast<std::string>(vid) + std::string(". Vid does not exist."));
      logstream(LOG_WARNING) << errormsg;
      oarc << false << errormsg;
    } else {
      std::vector<graph_edge*> _inadj;
      std::vector<graph_edge*> _outadj;
      std::vector<graph_edge*>* out_inadj = get_in ? &(_inadj) : NULL;
      std::vector<graph_edge*>* out_outadj = get_out ? &(_outadj) : NULL;

      v->get_adj_list(shardid, prefetch_data, out_inadj, out_outadj);

      size_t numin = get_in ? out_inadj->size() : 0;
      size_t numout = get_out ? out_outadj->size() : 0;
      oarc << true << numin << numout << prefetch_data;
      for (size_t i = 0; i < numin; i++) {
        graph_edge* e = out_inadj->at(i);
        oarc << e->get_src() << e->get_id();
        if (prefetch_data)
          oarc << *(e->data());
      }
      for (size_t i = 0; i < numout; i++) {
        graph_edge* e = out_outadj->at(i);
        oarc << e->get_dest() << e->get_id();
        if (prefetch_data)
          oarc << *(e->data());
      }
    }
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }


  graph_database* get_database() {
    return database;
  }
};
} // namespace graphlab
#endif
