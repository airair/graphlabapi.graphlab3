#ifndef GRAPHLAB_DATABASE_GRAPHDB_QUERY_OBJ_HPP
#define GRAPHLAB_DATABASE_GRAPHDB_QUERY_OBJ_HPP
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graphdb_config.hpp>
#include <graphlab/database/errno.hpp>

#include <fault/query_object_client.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
namespace graphlab {
  /**
   * \ingroup group_graph_database
   * Interface of a graph query_client. Provides functionality
   * for issue query/update request to graph_database_server.
   **/
  class graphdb_query_object {
   public:
    typedef libfault::query_object_client::query_result query_result;

    graphdb_query_object (graphdb_config& config);
  
    ~graphdb_query_object(); 

    // ------------ Query Interafce --------------------
    query_result query (graph_shard_id_t shardid, char* msg, size_t msg_len);

    query_result update (graph_shard_id_t shardid, char* msg, size_t msg_len); 

    void batch_query (graph_shard_id_t shardid, char* msg, size_t msg_len, std::vector<query_result>& reply_queue);

    void batch_update (graph_shard_id_t shardid, char* msg, size_t msg_len, std::vector<query_result>& reply_queue); 


    // query a random shard server
    query_result query_any (char* msg, size_t msg_len);

    void update_all (char* msg, size_t msg_len, std::vector<query_result>& reply_queue);

    void query_all (char* msg, size_t msg_len, std::vector<query_result>& reply_queue);

    void update_multi (std::vector<graph_shard_id_t>& shards,
                              char* msg, size_t msg_len,
                             std::vector<query_result>& reply_queue); 

    void query_multi (std::vector<graph_shard_id_t>& shards,
                             char* msg, size_t msg_len,
                             std::vector<query_result>& reply_queue); 

    // ----------- Reply Parsing Interface --------------------
     /**
      * Parse a reply that has no content. 
      */
     int parse_reply(query_result& future);

     /**
      * Parse a reply with single return content serialized into the out variable.
      */
     template<typename T>
     int parse_reply(query_result& future, T& out) {
       if (future.get_status() != 0) {
         logstream(LOG_ERROR) << glstrerr(ESRVUNREACH) << std::endl;
         return ESRVUNREACH;
       }
       std::string reply = future.get_reply();
       iarchive iarc(reply.c_str(), reply.length());
       int err = 0;
       iarc >> err ;
       if (err != 0) {
         logstream(LOG_ERROR) << glstrerr(err) << std::endl;
       } else {
         iarc >> out;
       }
       return err;
     }

     template<typename T>
     bool parse_batch_reply(query_result& future, std::vector<T>* out, std::vector<int>& errorcodes) {
       if (future.get_status() != 0) {
         logstream(LOG_ERROR) << glstrerr(ESRVUNREACH) << std::endl;
         return false;
       }
       std::string reply = future.get_reply();
       iarchive iarc(reply.c_str(), reply.length());
       bool success = false;
       iarc >> success;
       if (out != NULL)
         iarc >> *out;
       if (!success) {
         iarc >> errorcodes;
       }
       return success;
     }

     /// Parse a list of futures and aggregate the result into acc.
     /// The content of the result must be of the same type and supports +=.
     template<typename T>
     bool parse_and_aggregate(std::vector<query_result>& futures, T& acc,
                             std::vector<int>& errorcodes) {
       bool success = true;
       for (size_t i = 0; i < futures.size(); i++) {
         T next;
         if (futures[i].get_status() == 0) {
           int err = parse_reply(futures[i], next);
           if (err == 0) {
             acc += next;
           } else {
             errorcodes.push_back(err);
             success = false;
             logstream(LOG_ERROR) << glstrerr(err) << std::endl;
           }
         } else {
           errorcodes.push_back(ESRVUNREACH);
           success = false;
           logstream(LOG_ERROR) << glstrerr(ESRVUNREACH) << std::endl;
         }
       }
       return success;
     }

   private:
    // the actual query object which connects to the graph db server.
    libfault::query_object_client* qoclient;

    inline std::string find_server(graph_shard_id_t shardid) {
      return "shard"+boost::lexical_cast<std::string>(shardid);
    } 

    std::vector<graph_shard_id_t> shard_list;

    // rng seed
    boost::random::mt19937 rng;
  };
}
#endif
