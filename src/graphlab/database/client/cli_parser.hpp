#ifndef GRAPHLAB_DATABASE_CLI_PARSER_HPP
#define GRAPHLAB_DATABASE_CLI_PARSER_HPP
#include <graphlab/logger/logger.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <string>
#include <vector>
namespace graphlab {
  class cli_parser {
   public:
    enum commandtype_enum  {
      GET_TYPE,
      SET_TYPE,
      ADD_TYPE,
      BATCH_ADD_TYPE,
      BATCH_GET_TYPE,
      LOAD_TYPE,
      COMPUTE_TYPE,
      RESET_TYPE
    };

    enum targettype_enum {
      GRAPH_INFO_TYPE,
      VERTEX_TYPE,
      EDGE_TYPE,
      VERTEX_ADJ_TYPE,
      SHARD_TYPE,
      SHARD_ADJ_TYPE,
      VERTEX_FIELD_TYPE,
      EDGE_FIELD_TYPE,
      STRING_TYPE
    };

    struct parsed_command {
      commandtype_enum cmd_type;
      targettype_enum target_type;
      std::string value;
    };

    bool parse(std::istream& in, parsed_command& out);

    template<typename T>
    bool parse_singleton(std::string str, T& out, std::string errormsg = "") {
      try {
        out = boost::lexical_cast<T>(str);
        return true;
      } catch (boost::bad_lexical_cast &) {
        if (errormsg == "") {
          errormsg = "Error parsing singleton: " + str;
        }
        logstream(LOG_WARNING) << errormsg << std::endl;
        return false;
      }
    }

    template<typename T1, typename T2>
    bool parse_pair(std::string str, T1& out1, T2& out2, std::string delims,
                    std::string errormsg = "") {
      try {
        std::vector<std::string> strs;
        boost::split(strs, str, boost::is_any_of(delims));
        if (strs.size() != 2) {
          if (errormsg == "") {
            errormsg = "Error parsing pair: " + str;
          }
          logstream(LOG_WARNING) << errormsg << std::endl;
          return false;
        }
        out1 = boost::lexical_cast<T1>(strs[0]);
        out2 = boost::lexical_cast<T2>(strs[1]);
        return true;
      } catch (boost::bad_lexical_cast &) {
        if (errormsg == "") {
          errormsg = "Error parsing pair: " + str;
        }
        logstream(LOG_WARNING) << errormsg << std::endl;
        return false;
      }
    }

    template<typename T1, typename T2, typename T3>
    bool parse_triplet(std::string str, T1& out1, T2& out2, T3& out3,
                       std::string delims, std::string errormsg = "") {
      try {
        std::vector<std::string> strs;
        boost::split(strs, str, boost::is_any_of(delims));
        if (strs.size() != 3) {
          if (errormsg == "") {
            errormsg = "Error parsing triplets: " + str;
          }
          logstream(LOG_WARNING) << errormsg << std::endl;
          return false;
        }
        out1 = boost::lexical_cast<T1>(strs[0]);
        out2 = boost::lexical_cast<T2>(strs[1]);
        out3 = boost::lexical_cast<T3>(strs[2]);
        return true;
      } catch (boost::bad_lexical_cast &) {
        if (errormsg == "") {
          errormsg = "Error parsing triplets: " + str;
        }
        logstream(LOG_WARNING) << errormsg << std::endl;
        return false;
      }
    }


    template<typename T>
    bool parse_array(std::string str, std::vector<T>& outarr, std::string delims,
                     std::string errormsg = "") {
      try {
        std::vector<std::string> strs;
        boost::split(strs, str, boost::is_any_of(delims));
        for (size_t i = 0; i < strs.size(); i++)
          outarr.push_back(boost::lexical_cast<T>(strs[i]));
        return true;
      } catch (boost::bad_lexical_cast &) {
        if (errormsg == "") {
          errormsg = "Error parsing array: " + str;
        }
        logstream(LOG_WARNING) << errormsg << std::endl;
        return false;
      }
    }

   private:
    bool try_parse_cmd(std::string str, commandtype_enum& cmd);
    bool try_parse_target(std::string str, targettype_enum& target);
  };
}
#endif
