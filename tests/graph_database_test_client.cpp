#include <graphlab/database/client/distributed_graph_client.hpp>
#include <graphlab/database/client/graph_client_cli.hpp>
#include "graph_database_test_util.hpp"
using namespace std;

boost::unordered_map<graphlab::graph_shard_id_t, std::string> shard2server;
size_t nshards;
vector<graphlab::graph_field> vfields;
vector<graphlab::graph_field> efields;
typedef graphlab::graph_database_test_util test_util;
typedef libfault::query_object_client::query_result query_result;

int main(int argc, char** argv) {
  if (argc != 4) {
    cout << "Usage: graph_database_test_client [zkhost] [prefix] [config]\n";
    return 0;
  }

  string zkhost = argv[1];
  string prefix = argv[2];
  string config = argv[3];

  test_util::parse_config(config, vfields, efields, nshards, shard2server);

  vector<string> zkhosts; zkhosts.push_back(zkhost);
  graphlab::graph_shard_manager shard_manager(nshards);
  graphlab::distributed_graph_client graph(zkhosts, prefix, shard_manager, shard2server);
  graphlab::graph_client_cli graph_cli(&graph);

  graph_cli.get_graph_info();
  graph_cli.start();
}
