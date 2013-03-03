#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <fault/query_object_server.hpp>
#include <boost/lexical_cast.hpp>
#include "graph_database_test_util.hpp"
#include <vector>
using namespace std;

size_t nshards = 4;
vector<graphlab::graph_field> vfields;
vector<graphlab::graph_field> efields;

class shard_server : public libfault::query_object {

  graphlab::graph_database_server* server;
  graphlab::graph_database* db;

 public:
  shard_server(size_t shardid) {
    ASSERT_TRUE(shardid < nshards);
    db = graphlab::graph_database_test_util::createDatabase(shardid, vfields, efields, nshards);
    server = new graphlab::graph_database_server(db);
  }

  ~shard_server() {
    delete(db);
    delete(server);
  }

  void query(char* msg, size_t msglen,
             char** outreply, size_t *outreplylen) {
    std::string rep = server->query(msg, msglen);
    (*outreplylen) = rep.size();
    *outreply = (char*)malloc(*outreplylen);
    memcpy(*outreply, rep.c_str(), rep.size());
  }

  bool update(char* msg, size_t msglen,
              char** outreply, size_t *outreplylen) {
    std::string rep = server->update(msg, msglen);
    (*outreplylen) = rep.size();
    *outreply = (char*)malloc(*outreplylen);
    memcpy(*outreply, rep.c_str(), rep.size());
    return true;
  }

  void serialize(char** outbuf, size_t *outbuflen) { }

  void deserialize(const char* buf, size_t buflen) { }

  // object key is in the form of "shard$i"
  static query_object* factory(std::string objectkey, 
                               uint64_t create_flags) {
    size_t shardid = boost::lexical_cast<size_t>(objectkey.substr(5));
    return new shard_server(shardid);
  }
};

int main(int argc, char** argv)
{
  if (argc != 3) {
    std::cout << "Usage: graph_database_test_server [zkhost] [prefix] \n";
    return 0;
  }

  vfields.push_back(graphlab::graph_field("vdummy", graphlab::DOUBLE_TYPE));
  efields.push_back(graphlab::graph_field("edummy", graphlab::STRING_TYPE));

  std::string zkhost = argv[1];
  std::string prefix = argv[2];
  std::vector<std::string> zkhosts; zkhosts.push_back(zkhost);

  void* zmq_ctx = zmq_ctx_new();
  std::vector<std::string> masterkeys;
  masterkeys.push_back("shard0");
  masterkeys.push_back("shard1");
  masterkeys.push_back("shard2");
  masterkeys.push_back("shard3");

  libfault::query_object_server server(zmq_ctx, 2, 1);
  server.set_query_object_factory(shard_server::factory);
  server.register_zookeeper(zkhosts, prefix, "");
  server.set_all_object_keys(masterkeys);
  std::cout << "\n\n\n";
  server.start();
  while(1) {
    std::cout << "l: list objects\n";
    std::cout << "s [object]: stop managing object\n";
    std::cout << "q: quit\n";
    char command;
    std::cin >> command;
    if (command == 'q') break;
    else if (command == 'l'){
      std::vector<std::pair<std::string, size_t> > names = server.get_all_managed_object_names();
      for (size_t i = 0;i < names.size(); ++i) {
        std::cout << "\t" << names[i].first << ":" << names[i].second << std::endl;
      }
      std::cout << "\n";
    } else if(command == 's') {
      std::string objectname;
      std::cin >> objectname;
      server.stop_managing_object(objectname, 0);
      std::cout << "\n";
    }
  }
  server.stop();
}
