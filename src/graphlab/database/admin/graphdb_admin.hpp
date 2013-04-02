#include <graphlab/database/graphdb_config.hpp>
#include <fault/query_object_client.hpp>

namespace graphlab {
  class graphdb_admin {
    enum cmd_type {
      START,
      TERMINATE,
      UNKNOWN,
    };
    
   public:
     typedef libfault::query_object_client::query_result query_result;

     graphdb_admin(const graphdb_config& config) : config(config) { }

     bool process(std::string cmd_str) {
       return process(parse(cmd_str));
     }

   private:
     bool process(cmd_type cmd);

     cmd_type parse(std::string); 

   private:
     graphdb_config config;
  };
} // end of namespace
