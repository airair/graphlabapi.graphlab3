#include <graphlab/database/client/graphdb_client.hpp>
#include <graphlab/database/client/graph_loader.hpp>
#include <graphlab/database/graphdb_config.hpp>
#include <graphlab/logger/logger.hpp>

using namespace std;

int main(int argc, char** argv) {
  if (argc != 2) {
    cout << "Usage: graphdb_test_client [config]\n";
    return 0;
  }

  // string zkhost = argv[1];
  // string prefix = argv[2];
  string configfile = argv[1];

  graphlab::graphdb_config config(configfile);
  graphlab::graphdb_client client(config);
  graphlab::graph_loader loader(&client);
  loader.load_format("/Users/haijieg/data/google_graph/web-Google.txt", "snap");
  // loader.load_format("/Users/haijieg/data/google_graph/web-Google-tiny.txt", "snap");
  cout << "Num vertices: " << client.num_vertices() << endl;
  cout << "Num edges: " << client.num_edges() << endl;
  // vector<string> zkhosts; zkhosts.push_back(zkhost);

  // graphlab::graph_shard_manager shard_manager(nshards);
  // graphlab::distributed_graph_client graph(zkhosts, prefix, shard_manager, shard2server);
  // graphlab::graph_client_cli graph_cli(&graph);

  // graph_cli.get_graph_info();
  // graph_cli.start();
  
  return 0;
}
