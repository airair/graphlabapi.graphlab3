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
    std::string header;
    iarchive iarc(request, len);
    iarc >> header;
    oarchive oarc;
    if (header == "vertex_data") {
      set_vertex_field(iarc, oarc);
    } else if (header == "edge_data") {
      set_edge_field(iarc, oarc);
    } else {
      logstream(LOG_WARNING) <<  ("Unknown query header: " + header) << std::endl;
      oarc << false << ("Unknown query header: " + header);
    }

    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
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
    iarchive iarc(request, len);
    std::string header;
    iarc >> header;

    oarchive oarc;
    if (header == "vertex_fields_meta") {
      get_vertex_fields(iarc, oarc);
    } else if (header == "edge_fields_meta") {
      get_edge_fields(iarc, oarc);
    } else if (header == "sharding_graph") {
      get_sharding_constraint(iarc, oarc);
    } else if (header == "num_vertices") {
      get_num_vertices(iarc, oarc);
    } else if (header =="num_edges") {
      get_num_edges(iarc, oarc);
    } else if (header == "num_shards") {
      get_num_shards(iarc, oarc);
    } else if (header == "vertex_data_field") {
      get_vertex_data_field(iarc, oarc);
    } else if (header == "vertex_data_row") {
      get_vertex_data_row(iarc, oarc);
    } else if (header == "vertex_adj") {
      get_vertex_adj(iarc, oarc);
    } else if (header == "shard") {
      get_shard(iarc, oarc);
    }else {
      logstream(LOG_WARNING) <<  ("Unknown query header: " + header) << std::endl;
      oarc << false << ("Unknown query header: " + header);
    }

    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return  ret;
  }

  // ------------- Modification Handlers ---------------
  /**
   * Set the value of vertex (id=vid) at field fieldpos to the provided argument.
   * Return a false if the set operation failed. 
   */
  void set_vertex_field(iarchive& iarc, oarchive& oarc);

  /**
   * Set the value of edge (id=eid) in shard (id=shardid) at field fieldpos to the provided argument.
   * Return a false if the set operation failed. 
   */
  void set_edge_field(iarchive& iarc, oarchive& oarc);


  // ------------- Query Handlers ---------------

  /**
   * Returns a serialized string of the vertex field vector. 
   *
   * Serialization format:
   *  success << vector<graph_field>
   */
  void get_vertex_fields(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized string of the edge field vector.
   *
   * Serialization format:
   *  success << vector<graph_field>
   */
  void get_edge_fields(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serilaized string of number of edges
   *
   * Serialization format:
   *  success << num_edges 
   */
  void get_num_edges(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serilaized string of number of edges
   *
   * Serialization format:
   *  success << num_vertices 
   */
  void get_num_vertices(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serilaized string of number of edges
   *
   * Serialization format:
   *  success << num_shards
   */
  void get_num_shards(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized string of the sharding graph.
   *
   * Serialization format:
   *  success << sharding_graph
   */
  void get_sharding_constraint(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialized string of the entire row corresponding to the query vertex.
   *
   * Serialization format:
   *  success << row
   *
   */
  void get_vertex_data_row(iarchive& iarc, oarchive& oarc);

   /**
   * Returns the serialization of graph value corresponding to the query vertex and field index.
   *
   * Serialization format:
   *  success << graph_value 
   */
  void get_vertex_data_field(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialization of the adjacency structure corresponding to the 
   * query vertex and shard id.
   *
   * Serialization format:
   *  success << numin << numout << prefetch_data << ADJACENCY(in) << ADJACENCY(out)
   *  ADJACENCY format: e1.src << e1.dst [<< e1.data] << e2.src << e2.dst [<< d2.data]
   */
  void get_vertex_adj(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialization of the shards corresponding to the 
   * query shard id in iarc.
   *
   * Serialization format:
   *  success << shard 
   */
  void get_shard(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized error message string;
   */
  // std::string error_msg(std::string msg) {
  //   oarchive oarc;
  //   logstream(LOG_WARNING) << "GraphDB Server: " << msg;
  //   oarc << false << msg;
  //   std::string ret(oarc.buf, oarc.off);
  //   free(oarc.buf);
  //   return ret;
  // }

  graph_database* get_database() {
    return database;
  }

};
} // namespace graphlab
#endif
