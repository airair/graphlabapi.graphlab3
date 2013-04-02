#include <graphlab/database/admin/graphdb_admin.hpp>
#include <graphlab/database/errno.hpp>
#include <graphlab/database/graphdb_query_object.hpp>
#include <graphlab/database/query_message.hpp>
#include <iostream>

namespace graphlab {
  bool graphdb_admin::process(cmd_type cmd) { 
    switch (cmd) {
     case START: {
       std::vector<std::string> address = config.get_serveraddrs();
       return true;
     }
     case TERMINATE: {
       QueryMessage qm(QueryMessage::ADMIN, QueryMessage::TERMINATE);
       graphlab::graphdb_query_object qo(config);
       std::vector<query_result> results;
       qo.query_all(qm.message(), qm.length(), results);
       return true;
     }
     default: {
       logstream(LOG_WARNING) << glstrerr(EINVCMD) << std::endl;
       return false;
     }
    }
  }

  graphdb_admin::cmd_type graphdb_admin::parse(std::string str) {
    if (str == "start") {
      return START;
    } else if (str == "terminate") {
      return TERMINATE;
    } else {
      return UNKNOWN;
    }
  }
}

using namespace std;
int main(int argc, const char *argv[])
{
  if (argc != 3) {
    cout << "Usage graphdb_admin config [START | DESTROY]" << endl;
    return 0;
  }
  graphlab::graphdb_config config(argv[1]);
  graphlab::graphdb_admin admin(config);
  admin.process(argv[2]);
  return 0;
}
