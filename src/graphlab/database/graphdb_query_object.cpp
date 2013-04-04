#include <graphlab/database/graphdb_query_object.hpp>
#include <zmq.h>

namespace graphlab {

  typedef libfault::query_object_client::query_result query_result;

  graphdb_query_object::graphdb_query_object (const graphdb_config& config) {
    size_t nshards = config.get_nshards();
    for (size_t i = 0; i < nshards; ++i)
      shard_list.push_back(i);

    void* zmq_ctx = zmq_ctx_new();
    std::vector<std::string> zkhosts = config.get_zkhosts();
    std::string zkprefix = config.get_zkprefix();
    qoclient = new libfault::query_object_client(zmq_ctx, zkhosts, zkprefix);
  }

  graphdb_query_object::~graphdb_query_object() {
    delete qoclient;
  }

  // ------------ Query Interafce --------------------
  query_result graphdb_query_object::query (graph_shard_id_t shardid, char* msg, size_t msg_len) {
    return qoclient->query(find_server(shardid), msg, msg_len);
  }

  query_result graphdb_query_object::update (graph_shard_id_t shardid, char* msg, size_t msg_len) {
    return qoclient->update(find_server(shardid), msg, msg_len);
  }

  void graphdb_query_object::update_all (char* msg, size_t msg_len, std::vector<query_result>& reply_queue) {
    update_multi (shard_list, msg, msg_len, reply_queue);
  }

  void graphdb_query_object::query_all (char* msg, size_t msg_len,
                                        std::vector<query_result>& reply_queue) {
    query_multi(shard_list, msg, msg_len, reply_queue);
  }

  void graphdb_query_object::update_multi (std::vector<graph_shard_id_t>& shards,
                                           char* msg, size_t msg_len,
                                           std::vector<query_result>& reply_queue) {
    for (size_t i = 0; i < shards.size(); ++i) {
      char* msg_copy = (char*)malloc(msg_len);
      memcpy(msg_copy, msg, msg_len);
      query_result result = update(shards[i], msg_copy, msg_len);
      reply_queue.push_back(result);
    }
    free(msg);
  }

  void graphdb_query_object::query_multi (std::vector<graph_shard_id_t>& shards,
                                          char* msg, size_t msg_len,
                                          std::vector<query_result>& reply_queue) {
    for (size_t i = 0; i < shards.size(); ++i) {
      char* msg_copy = (char*)malloc(msg_len);
      memcpy(msg_copy, msg, msg_len);
      query_result result = query(shards[i], msg_copy, msg_len);
      reply_queue.push_back(result);
    }
    free(msg);
  }

  // query a random shard server
  query_result graphdb_query_object::query_any (char* msg, size_t msg_len) {
    boost::random::uniform_int_distribution<> runif(0,shard_list.size()-1);
    return qoclient->query(find_server(runif(rng)), msg, msg_len);
  }

  // ----------- Reply Parsing Interface --------------------
  /**
   * Parse a reply that has no content. 
   */
  int graphdb_query_object::parse_reply(query_result& future) {
    if (future.get_status() != 0) {
      logstream(LOG_ERROR) << glstrerr(ESRVUNREACH) << std::endl;
      return ESRVUNREACH;
    } else {
      std::string reply = future.get_reply();
      iarchive iarc(reply.c_str(), reply.length());
      int err = 0;
      iarc >> err;
      if (err != 0) {
        logstream(LOG_ERROR) << glstrerr(err) << std::endl;
      } 
      return err;
    }
  }
}
