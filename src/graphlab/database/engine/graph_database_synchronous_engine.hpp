#ifndef GRAPH_DATABASE_SYNCHRONOUS_ENGINE
#define GRAPH_DATABASE_SYNCHRONOUS_ENGINE
#include<graphlab/database/basic_types.hpp>
#include<graphlab/database/graph_value.hpp>
#include<graphlab/database/query_messages.hpp>
#include<graphlab/database/error_messages.hpp>
#include<fault/query_object_client.hpp>
#include<boost/unordered_map.hpp>

namespace graphlab {
  class graph_database; 
  class graph_client;
  class graph_vertex;
  class graph_vertex_remote; 

  typedef libfault::query_object_client::query_result query_result;

  class graph_database_synchronous_engine {
   public:
     /**
      * States for vertex programs
      */
     struct vertex_state{
       double acc;
       bool is_scheduled;
     };

     graph_database_synchronous_engine(graph_database* database,
                                       graph_client* query_client) : database(database), query_client(query_client) {}

    // initalize the engine. Initializing vertex states and
    // remote vertices that are adjacent to the local shards.
    void init();

    // Run the computation synchrounously for niter iterations.
    void run();

    // finalize the engine. Write back all changes. Free the resources.
    void finalize();

   private:
    graph_database* database;
    graph_client* query_client;

    void write_vertex_changes(size_t lvid, const graph_value& value,
                              std::vector<query_result>& reply_queue,
                              bool use_delta);

    size_t num_local_vertices;
    // vertex_program* updatefun;

    std::vector<graph_shard_id_t> remote_shards;
    std::vector<graph_shard_id_t> local_shards;

    std::vector<vertex_state> vstates; 
    std::vector<graph_vertex*> vertex_array;
    std::vector<graph_vertex_remote*> remote_vertex_vector_head;
    boost::unordered_map<graph_vid_t, size_t> vid2lvid;

    QueryMessages messages;
    ErrorMessages error_messages;
  };
}// end of namespace
#endif
