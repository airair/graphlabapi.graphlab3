#include <graphlab/database/graphdb_config.hpp>
#include <graphlab/logger/logger.hpp>

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include <fstream>
#include <iostream>

namespace graphlab {
  bool graphdb_config::parse(std::string fname) {
    logstream(LOG_EMPH) << "Parse config file: " << fname << "\n";
    // bool success;
    std::ifstream in(fname.c_str());
    // parse zookeeper info
    if (!parse_zkinfo(in)) {
      in.close();
      logstream(LOG_FATAL) << "Error parsing config. Invalid zookeeper hosts and prefix" << std::endl; 
      return false;
    }
    
    // parse number of shards
    in >> nshards;
    logstream(LOG_EMPH) << "nshards: " << nshards << std::endl; 

    // success &= parse_fields(in, vertex_fields);
    // success &= parse_fields(in, edge_fields);
//     if (!success) {
//         logstream(LOG_ERROR) << "Error parsing config. Invalid vertex or edge fields." << std::endl; 
    //     return false;
    // }

    in.close();

    // logstream(LOG_EMPH) << "vertex fields: " << "\n";
    // for (size_t i = 0; i < vertex_fields.size(); i++) {
    //   logstream(LOG_EMPH) << vertex_fields[i] << "\t";
    // }
    // logstream(LOG_EMPH) << "\nedge fields: " << "\n";
    // for (size_t i = 0; i < edge_fields.size(); i++) {
    //   logstream(LOG_EMPH) << edge_fields[i] << "\t";
    // }
    logstream(LOG_EMPH) << "====== Done parsing config =========" << std::endl;

    return true;
  }

  bool graphdb_config::parse_zkinfo(std::ifstream& in) {
    std::string line;
    while(getline(in, line)) {
      if (line != "")
        break;
    }
    if (line == "") {
      return false;
    } else {
      std::vector<std::string> strs;
      boost::split(strs, line, boost::is_any_of(" "));
      if (strs.size() < 2) {
        return false;
      }
      for (size_t i = 0; i < strs.size()-1; i++) {
        zk_hosts.push_back(strs[i]);
      }
      zk_prefix = strs[strs.size()-1];

      logstream(LOG_EMPH) << "zookeeper: " << line << std::endl;
      return true;
    }
  }

  // bool graphdb_config::parse_fields(std::ifstream& in, std::vector<graph_field>& fields) {
  //   std::string line;
  //   while(getline(in, line)){  
  //     if (line != "")
  //       break;
  //   }
  //   if (line == "") {
  //     return true;
  //   } else {
  //     std::vector<std::string> strs;
  //     boost::split(strs, line, boost::is_any_of(","));
  //     for (size_t i = 0; i < strs.size(); i++) {
  //       std::vector<std::string> pair;
  //       boost::split(pair, strs[i], boost::is_any_of(":"));
  //       graph_field field;
  //       if (pair.size() != 2) {
  //         return false;
  //       }
  //       field.name = pair[0];
  //       field.type = string_to_type(pair[1]);
  //       if (field.type == unknown_type)
  //         return false;
  //       fields.push_back(field);
  //     }
  //     return true;
  //   }
  // }
} // end of namespace
