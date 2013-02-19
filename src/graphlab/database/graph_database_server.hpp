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

  /**
   * Handle the SET queries.
   *
   * Current support queries:
   *  // Set the vertex data at a given field.
   *  header("vertex_data") >> vid >> fieldpos >> len >> data
   *
   *  // Set the edge data at a given field.
   *  header("edge_data") >> eid >> shardid >> fieldpos >> len >> data
   */
  std::string update(const char* request, size_t len) {
    std::string ret;
    iarchive iarc(request, len);
    std::string header;
    iarc >> header;
 
    if (header == "vertex_data") {
      graph_vid_t vid;
      size_t fieldpos, len;
      iarc >> vid >> fieldpos >> len;
      char* val = (char*)malloc(len);
      iarc.read(val, len);
      ret = set_vertex_field(vid, fieldpos, val, len);
    } else if (header == "edge_data") {
      graph_eid_t eid;
      graph_shard_id_t shardid;
      size_t fieldpos, len;
      iarc >> eid >> fieldpos >> len;
      char* val = (char*)malloc(len);
      iarc.read(val, len);
      set_edge_field(eid, shardid, fieldpos, val, len);
    } else {
      ret = error_msg("Unknown query header" + header);
    }
    return ret;
  } 

  /**
   * Handle the GET queries.
   *
   * Current support queries:
   *  // get vertex fields metadata
   *  header("vertex_fields_meta")      
   *
   *  // get edge_fields metat data
   *  header("edge_fields_meta")
   *
   *  // get the value of a vertex at a given field
   *  header("vertex_data_field") >> vid >> fieldpos  
   *
   *  // get the entire row of a vertex
   *  header("vertex_data_row") >> vid >> row             
   *
   *  // get  the adjacency edges of vertex at a given shard. Boolean options: getin, getout, and prefetch_data. 
   *  header("vertex_adj") >> vid >> shardid >> getin >> getout >> prefetch_data     
   */
  std::string query(const char* request, size_t len) {
    std::string ret;
    iarchive iarc(request, len);
    std::string header;
    iarc >> header;
    if (header == "vertex_fields_meta") {
      ret = get_vertex_fields();
    } else if (header == "edge_fields_meta") {
      ret = get_edge_fields();
    } else if (header == "vertex_data_field") {
      graph_vid_t vid;
      size_t fieldpos;
      iarc >> vid >> fieldpos;
      ret = get_vertex_data(vid, fieldpos);
    } else if (header == "vertex_data_row") {
      graph_vid_t vid;
      iarc >> vid;
      ret = get_vertex_data(vid);
    } else if (header == "vertex_adj") {
      graph_vid_t vid;
      graph_shard_id_t shardid;
      bool get_in, get_out, prefetch_data;
      iarc >> vid >> shardid >> get_in >> get_out >> prefetch_data;
      ret = get_vertex_adj(vid, shardid, get_in, get_out, prefetch_data);
    } else {
      ret = error_msg("Unknown query header" + header);
    }
    return  ret;
  }

  // ------------- Modification Handlers ---------------
  /**
   * Set the value of vertex (id=vid) at field fieldpos to the provided argument.
   * Return a false if the set operation failed. 
   */
  std::string set_vertex_field(graph_vid_t vid, size_t fieldpos, const char* val, size_t len);

  /**
   * Set the value of edge (id=eid) in shard (id=shardid) at field fieldpos to the provided argument.
   * Return a false if the set operation failed. 
   */
  std::string set_edge_field(graph_eid_t eid, graph_shard_id_t shardid, size_t fieldpos, const char* val, size_t len);


  // ------------- Query Handlers ---------------

  /**
   * Returns a serialized string of the vertex field vector. 
   *
   * Serialization format:
   *  success << vector<graph_field>
   */
  std::string get_vertex_fields();

  /**
   * Returns a serialized string of the edge field vector.
   *
   * Serialization format:
   *  success << vector<graph_field>
   */
  std::string get_edge_fields();

  /**
   * Returns the serialized string of the entire row corresponding to the query vertex.
   *
   * Serialization format:
   *  success << row
   *
   */
  std::string get_vertex_data(graph_vid_t vid);

   /**
   * Returns the serialization of graph value corresponding to the query vertex and field index.
   *
   * Serialization format:
   *  success << graph_value 
   */
  std::string get_vertex_data(graph_vid_t vid, size_t fieldpos);

  /**
   * Returns the serialization of the adjacency structure corresponding to the 
   * query vertex and shard id.
   *
   * Serialization format:
   *  success << numin << numout << prefetch_data << ADJACENCY(in) << ADJACENCY(out)
   *  ADJACENCY format: e1.src << e1.dst [<< e1.data] << e2.src << e2.dst [<< d2.data]
   */
  std::string get_vertex_adj(graph_vid_t vid, graph_shard_id_t shardid, bool get_in, bool get_out, bool prefetch_data);

  /**
   * Returns a serialized error message string;
   */
  std::string error_msg(std::string msg) {
    oarchive oarc;
    logstream(LOG_WARNING) << "GraphDB Server: " << msg;
    oarc << false << msg;
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
