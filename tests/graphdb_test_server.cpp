#include <graphlab/database/server/graphdb_server.hpp>
#include <graphlab/logger/logger.hpp>

#include <fault/query_object.hpp>
#include <fault/query_object_server_process.hpp>
#include <boost/lexical_cast.hpp>

#include <vector>

using namespace graphlab;
static libfault::query_object* factory(std::string objectkey, 
                                       std::vector<std::string> zkhosts,
                                       std::string prefix,
                                       uint64_t create_flags) {
  logstream(LOG_EMPH) << "Create graph_shard_server: shardid = " << objectkey << std::endl;
  graph_shard_id_t shardid = boost::lexical_cast<graph_shard_id_t>(objectkey);
  bool is_master = (create_flags & QUERY_OBJECT_CREATE_MASTER);
  graphdb_server* server = new graphdb_server(shardid, is_master);
  return server;
}

int main(int argc, char** argv)
{
  libfault::query_main(argc, argv, factory);
  return 0;
}
