#include <graphlab/database/admin/graphdb_admin.hpp>
#include <graphlab/database/errno.hpp>
#include <graphlab/database/graphdb_query_object.hpp>
#include <graphlab/database/query_message.hpp>
#include <fault/query_object_server_manager.hpp>
#include <iostream>

namespace graphlab {

  void graphdb_admin::start_server(std::string serverbin) {
    // TODO: read from config 
    size_t replicacount = 0;
    size_t objectcap = 2;
    size_t max_masters = 1;

    libfault::query_object_server_manager manager(serverbin, replicacount, objectcap);
    manager.register_zookeeper(config.get_zkhosts(), config.get_zkprefix());

    std::vector<std::string> objectkeys;
    for (size_t i = 0; i < config.get_nshards(); ++i) {
      objectkeys.push_back(boost::lexical_cast<std::string>(i));
    } 
    manager.set_all_object_keys(objectkeys);

    std::cout << "\n\n\n";
    manager.start(max_masters);
    while(1) {
      std::cout << "l: list objects\n";
      std::cout << "s [object]: stop managing object\n";
      std::cout << "q: quit\n";
      char command;
      std::cin >> command;
      if (command == 'q') break;
      else if (command == 'l'){
        manager.print_all_object_names();
      } else if(command == 's') {
        std::string objectname;
        std::cin >> objectname;
        manager.stop_managing_object(objectname);
        std::cout << "\n";
      }
    }
    manager.stop();
  }

  bool graphdb_admin::process(cmd_type cmd, int argc, const char* argv[]) { 
    switch (cmd) {
     case START: {
       if (argc < 1) {
         std::cout << "Server serverbin not provided. Abort" << std::endl;
         return false;
       } else {
         start_server(std::string(argv[0]));
         std::cout << argv[0];
         return true;
       }
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
  if (argc < 3) {
    cout << "Usage graphdb_admin config [START | DESTROY] [args...]" << endl;
    return 0;
  }
  graphlab::graphdb_config config(argv[1]);
  graphlab::graphdb_admin admin(config);
  admin.process(argc-2, argv+2);
  return 0;
}