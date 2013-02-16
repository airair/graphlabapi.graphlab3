#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
using namespace std;
int main(int argc, char** argv)
{
  graphlab::graph_database_sharedmem db;
  graphlab::graph_database_server server(&db);
  return 0;
}

