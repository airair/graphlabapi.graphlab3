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
#include <graphlab/database/query_messages.hpp>
#include <graphlab/serialization/oarchive.hpp>

#include <graphlab/database/distributed_graph/distributed_graph_vertex.hpp>

#include <fault/query_object_client.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/join.hpp>

namespace graphlab {
/**
 * \ingroup group_graph_database
 * An abstract interface for a graph database implementation 
 */
class graph_database_server {

  // Pointer to the underlying database.
  graph_database* database;

  // Defines the communication protocols. 
  QueryMessages messages;

  // Query object client that can talk to other servers. 
  libfault::query_object_client* qoclient;

  // A list of names of all shard servers.
  std::vector<std::string> server_list;

  typedef libfault::query_object_client::query_result query_result;

 public:

  /**
   * Creates a shard server which is not able to talk to other servers.
   */
  graph_database_server(graph_database* db) : database(db), qoclient(NULL) { 
    ASSERT_TRUE(db != NULL);
    server_list.push_back("local");
  }

  /**
   * Creates a shard server with query_object_client configuration.
   */
  graph_database_server(graph_database* db,
                        void* zmq_ctx,
                        std::vector<std::string> zkhosts,
                        std::string prefix,
                        const std::vector<std::string> server_list) : 
      database(db), server_list(server_list) { 
    ASSERT_TRUE(db != NULL);
    qoclient = new libfault::query_object_client(zmq_ctx, zkhosts, prefix);
  }

  virtual ~graph_database_server() {
    if (qoclient != NULL) delete qoclient;
  }

  /**
   * Handle the SET queries.
   * This function does not free the request message.
   */
  std::string update(char* request, size_t len) {
    std::string header;
    iarchive iarc(request, len);
    iarc >> header;
    oarchive oarc;
    if (header == "vertex_field") {
      set_vertex_field(iarc, oarc);
    } else if (header == "edge_field") {
      set_edge_field(iarc, oarc);
    } else if (header == "vertex_row") {
      set_vertex_row(iarc, oarc);
    } else if (header == "edge_row") {
      set_edge_row(iarc, oarc);
    } else if (header == "add_vertex") {
      add_vertex(iarc, oarc);
    } else if (header == "batch_add_vertex") {
      batch_add_vertex(iarc, oarc);
    } else if (header == "add_edge") {
      add_edge(iarc, oarc);
    } else if (header == "batch_add_edge") {
      batch_add_edge(iarc, oarc);
    } else if (header == "add_vertex_mirror") {
      add_vertex_mirror(iarc, oarc);
    } else if (header == "batch_add_vertex_mirror") {
      batch_add_vertex_mirror(iarc, oarc);
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
   * This function does not free the request message.
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
    } else if (header == "vertex") {
      get_vertex(iarc, oarc);
    } else if (header == "edge") {
      get_edge(iarc, oarc);
    }else if (header == "vertex_data_field") {
      get_vertex_data_field(iarc, oarc);
    } else if (header == "vertex_data_row") {
      get_vertex_data_row(iarc, oarc);
    } else if (header == "vertex_adj") {
      get_vertex_adj(iarc, oarc);
    } else if (header == "shard") {
      get_shard(iarc, oarc);
    } else if (header == "shard_adj") {
      get_shard_contents_adj_to(iarc, oarc);
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
   * Reply is header only.
   */
  void set_vertex_field(iarchive& iarc, oarchive& oarc);

  /**
   * Set the value of edge (id=eid) in shard (id=shardid) at field fieldpos to the provided argument.
   * Reply is header only.
   */
  void set_edge_field(iarchive& iarc, oarchive& oarc);

  /**
   * Set the entire row of vertex (id=vid) to the provided argument.
   * Reply is header only.
   */
  void set_vertex_row(iarchive& iarc, oarchive& oarc);

  /**
   * Set the entire row of vertex (id=vid) to the provided argument.
   * Reply is header only.
   */
  void set_edge_row(iarchive& iarc, oarchive& oarc);


  // ------------- Query Handlers ---------------

  /**
   * Returns a serialized string of the vertex field vector. 
   * Reply format:
   *  success << vector<graph_field>
   */
  void get_vertex_fields(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized string of the edge field vector.
   * Reply format:
   *  success << vector<graph_field>
   */
  void get_edge_fields(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serilaized string of number of edges
   * Reply format:
   *  success << num_edges 
   */
  void get_num_edges(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serilaized string of number of edges
   * Reply format:
   *  success << num_vertices 
   */
  void get_num_vertices(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serilaized string of number of edges
   * Reply format:
   *  success << num_shards
   */
  void get_num_shards(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized string of the sharding graph.
   * Reply format:
   *  success << sharding_graph
   */
  void get_sharding_constraint(iarchive& iarc, oarchive& oarc);


  /**
   * Returns a serialized string of the query vertex.
   * Reply format:
   *  success << distributed_graph_vertex.
   */
  void get_vertex(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized string of the query edge.
   * Reply format:
   *  success << distributed_graph_edge.
   */
  void get_edge(iarchive& iarc, oarchive& oarc);


  /**
   * Returns the serialized string of the entire row corresponding to the query vertex.
   * Reply format:
   *  success << row
   */
  void get_vertex_data_row(iarchive& iarc, oarchive& oarc);

   /**
   * Returns the serialization of graph value corresponding to the query vertex and field index.
   * Reply format:
   *  success << graph_value 
   */
  void get_vertex_data_field(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialization of the adjacency structure of the 
   * query vertex on a local shard with shard id.
   * The query vertex may not be local.
   *
   * Reply format:
   *  success << numin << numout 
   *          << std::vector<distributed_graph_edge::vertex_adjacency_record>
   *          << std::vector<distributed_graph_edge::vertex_adjacency_record> 
   */
  void get_vertex_adj(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialization of the shards corresponding to the 
   * query shard id in iarc.
   * Reply format:
   *  success << graph_shard 
   */
  void get_shard(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialization of the shard corresponding to the 
   * query adjacent shard ids in iarc.
   * Reply format:
   *  success << graph_shard 
   */
  void get_shard_contents_adj_to(iarchive& iarc, oarchive& oarc);


  // ---------------- Ingress API -----------------
  void add_vertex(iarchive& iarc, oarchive& oarc);

  void batch_add_vertex(iarchive& iarc, oarchive& oarc);

  void add_edge(iarchive& iarc, oarchive& oarc);

  void batch_add_edge(iarchive& iarc, oarchive& oarc);

  void add_vertex_mirror(iarchive& iarc, oarchive& oarc);

  void batch_add_vertex_mirror(iarchive& iarc, oarchive& oarc);

  graph_database* get_database() {
    return database;
  }
};
} // namespace graphlab
#endif
