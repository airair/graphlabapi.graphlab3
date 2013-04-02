#ifndef GRAPHLAB_DATABASE_GRAPHDB_SERVER_HPP
#define GRAPHLAB_DATABASE_GRAPHDB_SERVER_HPP
#include <graphlab/database/server/graph_shard_server.hpp>
#include <graphlab/database/query_message.hpp>
#include <graphlab/database/errno.hpp>

#include <fault/query_object.hpp>

namespace graphlab {

class graphdb_server : public libfault::query_object {

public:
  graphdb_server(size_t shardid, bool is_master = true) 
      : server(shardid), is_master(is_master) {}

  virtual ~graphdb_server() { }

  void query(char* msg, size_t msglen, char** outreply, size_t *outreplylen);

  bool update(char* msg, size_t msglen, char** outreply, size_t *outreplylen);

  void upgrade_to_master() {
    std::cout << "Upgrade to master\n";
    is_master = true;
  }

  void serialize(char** outbuf, size_t *outbuflen) { }

  void deserialize(const char* buf, size_t buflen) { }

 private:

  bool process(char* msg, size_t msglen, oarchive& oarc);

  int process_get(QueryMessage& qm, oarchive& oarc);
  int process_set(QueryMessage& qm, oarchive& oarc);
  int process_add(QueryMessage& qm, oarchive& oarc);

  int process_admin(QueryMessage& qm, oarchive& oarc);

  bool process_batch_get(QueryMessage& qm, oarchive& oarc);
  bool process_batch_set(QueryMessage& qm, oarchive& oarc);
  bool process_batch_add(QueryMessage& qm, oarchive& oarc);


  void terminate() { exit(0); }

 private:
  graphlab::graph_shard_server server;
  bool is_master;
  size_t counter;
};
} // end of namespace
#endif
