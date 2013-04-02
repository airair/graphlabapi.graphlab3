#include <graphlab/database/client/graphdb_client.hpp>
#include <graphlab/database/client/ingress/graph_loader.hpp>
#include <graphlab/database/graphdb_config.hpp>
#include <graphlab/database/util/graphdb_util.hpp>

using namespace std;

int main(int argc, char** argv) {
  if (argc != 4) {
    cout << "Usate: graphdb_ingress_test [config] [graph] [format]\n";
    return 0;
  }

  string configfile = argv[1];
  string graphfile = argv[2];
  string format = argv[3];

  graphlab::graphdb_config config(configfile);
  graphlab::graphdb_client client(config);
  graphlab::graph_loader loader(&client);

  graphlab::timer ti;
  ti.start();
  loader.load_from_posixfs(graphfile, format);
  cout << "Ingress completed in " << ti.current_time() << " secs" << endl;
}
