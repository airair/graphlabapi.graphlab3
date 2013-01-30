#include <graphlab/database/graph_database_sharedmem.hpp>
#include <vector>
using namespace std;
int main(int argc, char** argv) {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  int nshards = 4;
  graphlab::graph_database_sharedmem db(vertexfields, edgefields, nshards);
}
