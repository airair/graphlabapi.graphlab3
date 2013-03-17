#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_SERVER_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_SERVER_HPP
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <graphlab/database/query_messages.hpp>
#include <graphlab/database/error_messages.hpp>
#include <graphlab/database/engine/graph_database_synchronous_engine.hpp>
#include <fault/query_object_client.hpp>

namespace graphlab {
  class graph_database;
  class graph_client;

/**
 * \ingroup group_graph_database
 * A wrap around graph_database to provide query/update messaging interfaces.
 */
class graph_database_server {

  // Pointer to the underlying database.
  graph_database* database;

  // Graph query object client which communicates to other servers.
  graph_client* query_client;

  // Defines the communication protocols between server and client. 
  QueryMessages messages;

  // Defines the error messages when query fails. 
  ErrorMessages error_messages;

  typedef libfault::query_object_client::query_result query_result;

 public:
  /**
   * Creates a standalone server which does not communicate to other servers.
   */
  graph_database_server(graph_database* db) : 
      database(db) { }

  /**
   * Creates a distributed server.
   */
  graph_database_server(graph_database* db,
                        graph_client* client)
      : database(db), query_client(client) { 

    ASSERT_TRUE(db != NULL);
    ASSERT_TRUE(client != NULL);
  }

  virtual ~graph_database_server() { }

  /**
   * Handle updates. 
   * This function does not free the request message.
   */
  std::string update(char* request, size_t len) {
    std::string header;
    iarchive iarc(request, len);
    iarc >> header;
    oarchive oarc;
    if (header == "s_vertex_field") {
      set_vertex_field(iarc, oarc);
    } else if (header == "s_edge_field") {
      set_edge_field(iarc, oarc);
    } else if (header == "s_vertex_row") {
      set_vertex_row(iarc, oarc);
    } else if (header == "s_edge_row") {
      set_edge_row(iarc, oarc);
    } else if (header == "gc_add_vertex") {
      add_vertex(iarc, oarc);
    }  else if (header == "gc_add_edge") {
      add_edge(iarc, oarc);
    } else if (header == "gc_add_vertex_mirror") {
      add_vertex_mirror(iarc, oarc);
    } else if (header == "gc_add_field") {
      add_field(iarc, oarc);
    } else if (header == "gc_remove_field") {
      remove_field(iarc, oarc);
    } else if (header == "gc_reset_field") {
      reset_field(iarc, oarc);
    }// Batch updates
    else if (header == "gc_batch_add_vertex") {
      batch_add_vertex(iarc, oarc);
    } else if (header == "gc_batch_add_edge") {
      batch_add_edge(iarc, oarc);
    } else if (header == "gc_batch_add_vertex_mirror") {
      batch_add_vertex_mirror(iarc, oarc);
    } else {
      std::string message = error_messages.unknown_query_header(header);
       logstream(LOG_WARNING) << message << std::endl;
       oarc << false << message;
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
    // Temporary overiding query to deal with non-blocking update
    if (header == "s_vertex_field") {
      set_vertex_field(iarc, oarc);
    } else if (header == "s_edge_field") {
      set_edge_field(iarc, oarc);
    } else if (header == "s_vertex_row") {
      set_vertex_row(iarc, oarc);
    } else if (header == "s_edge_row") {
      set_edge_row(iarc, oarc);
    } // computing 
    else if (header == "compute") {
      compute(iarc, oarc);
    }
    // Usual queries
    else if (header == "g_vertex_fields_meta") {
      get_vertex_fields(iarc, oarc);
    } else if (header == "g_edge_fields_meta") {
      get_edge_fields(iarc, oarc);
    } else if (header == "g_num_vertices") {
      get_num_vertices(iarc, oarc);
    } else if (header =="g_num_edges") {
      get_num_edges(iarc, oarc);
    } else if (header == "g_num_shards") {
      get_num_shards(iarc, oarc);
    } else if (header =="g_shard_list") {
      get_shard_list(iarc, oarc);
    } else if (header == "g_vertex_adj_size") {
      get_vertex_adj_size(iarc, oarc);
    } else if (header == "g_vertex") {
      get_vertex(iarc, oarc);
    }  else if (header == "g_edge") {
      get_edge(iarc, oarc);
    } else if (header == "g_vertex_row") {
      get_vertex_data_row(iarc, oarc);
    } else if (header == "g_edge_row") {
      get_edge_data_row(iarc, oarc);
    }else if (header == "g_vertex_adj") {
      get_vertex_adj(iarc, oarc);
    } else if (header == "g_shard") {
      get_shard(iarc, oarc);
    } else if (header == "g_shard_adj") {
      get_shard_contents_adj_to(iarc, oarc);
    }  // Batch Queries
    else if (header == "g_batch_vertex") {
      batch_get_vertices(iarc, oarc);
    } else if (header == "g_vertex_shard_adj") {
      batch_get_vertex_adj_to_shard(iarc, oarc);
    } else {
      std::string message = error_messages.unknown_query_header(header);
      logstream(LOG_WARNING) << message << std::endl;;
      oarc << false << message;
    }
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return  ret;
  }

  // ------------- Modification Handlers ---------------
  /**
   * Add field to vertex or edge data.
   */
  void add_field(iarchive& iarc, oarchive& oarc);

  /**
   * Remove field from vertex or edge data. 
   */
  void remove_field(iarchive& iarc, oarchive& oarc);

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

  /**
   * Reset given fields to NULL.
   */
  void reset_field(iarchive& iarc, oarchive& oarc);


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

  void get_shard_list(iarchive& iarc, oarchive& oarc);

  void get_vertex_adj_size(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized string of the query vertex.
   * Reply format:
   *  success << distributed_graph_vertex.
   */
  void get_vertex(iarchive& iarc, oarchive& oarc);

  /**
   * Returns a serialized string of a list of vertices that are adjacent to the given shard.
   * Reply format:
   *  success << size << [v0, v1, v2...].
   */
  void batch_get_vertex_adj_to_shard(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialized string of the rows corresponding to the batch queried vertieces. 
   * If some query failed (e.g. the vertex does not exist), header is set to false, return succeeded queires followed by error messages.
   * Reply format:
   *  success << size << [v0, v1, v2 ...]
   *  fail << size << [v0, v1, v2 ...] << std::vector<errormsg>
   */
  void batch_get_vertices(iarchive& iarc, oarchive& oarc);


  /**
   * Returns a serialized string of the query edge.
   * Reply format:
   *  success << _graph_edge.
   */
  void get_edge(iarchive& iarc, oarchive& oarc);


  /**
   * Returns the serialized string of the entire row corresponding to the query vertex.
   * Reply format:
   *  success << row
   */
  void get_vertex_data_row(iarchive& iarc, oarchive& oarc);


  /**
   * Returns the serialized string of the entire row corresponding to the query edge.
   * Reply format:
   *  success << row
   */
  void get_edge_data_row(iarchive& iarc, oarchive& oarc);

  /**
   * Returns the serialization of the adjacency structure of the 
   * query vertex on a local shard with shard id.
   * The query vertex may not be local.
   *
   * Reply format:
   *  success << numin << numout 
   *          << std::vector<_graph_edge::vertex_adjacency_record>
   *          << std::vector<_graph_edge::vertex_adjacency_record> 
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

  void add_edge(iarchive& iarc, oarchive& oarc);

  void add_vertex_mirror(iarchive& iarc, oarchive& oarc);

  void batch_add_vertex(iarchive& iarc, oarchive& oarc);

  void batch_add_edge(iarchive& iarc, oarchive& oarc);

  void batch_add_vertex_mirror(iarchive& iarc, oarchive& oarc);


  // -------------- Computation Engine --------------
  void compute(iarchive& iarc, oarchive& oarc);

  // ---------------------------------------------------
  graph_database* get_database() {
    return database;
  }

  graph_client* get_query_client() {
    return query_client;
  }
};
} // namespace graphlab
#endif
