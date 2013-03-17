#include <graphlab/database/server/graph_database_server.hpp>
#include <graphlab/database/client/distributed_graph_client.hpp>
#include <graphlab/database/server/pagerank_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <fault/query_object_server_process.hpp>
#include <boost/lexical_cast.hpp>
#include "graph_database_test_util.hpp"
#include <vector>
using namespace std;

size_t nshards = 4;
vector<graphlab::graph_field> vfields;
vector<graphlab::graph_field> efields;
boost::unordered_map<graphlab::graph_shard_id_t, string> shard2server;

typedef graphlab::graph_database_test_util test_util;

class pagerank_test_server: public libfault::query_object {
 private:
  bool is_master;
  size_t counter;

  graphlab::pagerank_server* pr_server;
  graphlab::graph_database_server* base_server;
  graphlab::graph_client* client;
  graphlab::graph_database* db;

 public:
  pagerank_test_server(size_t shardid,
               const std::vector<std::string>& zkhosts,
               std::string& prefix) {
    counter = 0;
    db = test_util::createDatabase(shardid, vfields, efields, nshards);
    graphlab::graph_shard_manager shard_manager(nshards);
    client = new graphlab::distributed_graph_client(zkhosts, prefix, shard_manager, shard2server); 
    base_server = new graphlab::graph_database_server(db, client);
    pr_server= new graphlab::pagerank_server(base_server);
  }

  ~pagerank_test_server() {
    delete(pr_server);
    delete(base_server);
    delete(db);
    delete(client);
  }

  void query(char* msg, size_t msglen,
             char** outreply, size_t *outreplylen) {
    std::string rep = pr_server->query(msg, msglen);
    (*outreplylen) = rep.size();
    *outreply = (char*)malloc(*outreplylen);
    memcpy(*outreply, rep.c_str(), rep.size());
  }

  bool update(char* msg, size_t msglen,
              char** outreply, size_t *outreplylen) {
    std::string rep = pr_server->update(msg, msglen);
    (*outreplylen) = rep.size();
    *outreply = (char*)malloc(*outreplylen);
    memcpy(*outreply, rep.c_str(), rep.size());
    ++counter;
    return true;
  }

  void upgrade_to_master() {
    std::cout << "Upgrade to master\n";
    is_master = true;
  }

  void serialize(char** outbuf, size_t *outbuflen) { }

  void deserialize(const char* buf, size_t buflen) { }

  // object key is in the form of "shard$i"
  static query_object* factory(std::string objectkey, 
                               std::vector<std::string> zkhosts,
                               std::string prefix,
                               uint64_t create_flags) {
    // size_t shardid = boost::lexical_cast<size_t>(objectkey.substr(5));
    graphlab::graph_shard_id_t shardid = -1;
    typedef boost::unordered_map<graphlab::graph_shard_id_t, string>::iterator iterator;
    // find the shard id mapped to this server
    iterator it = shard2server.begin();
    while (it != shard2server.end()) {
      if (it->second == objectkey) {
        shardid = it->first;
        break;
      }
      it++;
    }
    if (shardid == -1) {
      cout << "cannot find shard id for server name " << objectkey << endl; 
      exit(0);
    }
    pagerank_test_server* server = new pagerank_test_server(shardid, zkhosts, prefix);
    server->is_master = (create_flags & QUERY_OBJECT_CREATE_MASTER);
    return server;
  }
};

int main(int argc, char** argv)
{
  string fname = argv[argc-1];  // the config file
  test_util::parse_config(fname, vfields, efields, nshards, shard2server);
  libfault::query_main(argc-1, argv, pagerank_test_server::factory);
  return 0;
}
