#include<graphlab/database/admin/graphdb_admin.hpp>

using namespace std;
int main(int argc, const char *argv[])
{
  if (argc < 3) {
    cout << "Usage graphdb_admin config [START | RESET] [args...]" << endl;
    return 0;
  }
  graphlab::graphdb_config config(argv[1]);
  graphlab::graphdb_admin admin(config);
  admin.process(argc-2, argv+2);
  return 0;
}
