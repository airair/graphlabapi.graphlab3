/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */
#ifndef GRAPHLAB_GRAPH_BUILTIN_PARSERS_HPP
#define GRAPHLAB_GRAPH_BUILTIN_PARSERS_HPP

#include <string>
#include <sstream>
#include <iostream>

#if defined(__cplusplus) && __cplusplus >= 201103L
// do not include spirit
#else
#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#endif

#include <graphlab/util/stl_util.hpp>
#include <graphlab/logger/logger.hpp>
#include <graphlab/serialization/serialization_includes.hpp>
#include <graphlab/database/basic_types.hpp>

namespace graphlab {

  namespace builtin_parsers {
    /**
     * \brief Parse files in the Stanford Network Analysis Package format.
     *
     * example:
     *
     *  # some comment
     *  # another comment
     *  1 2
     *  3 4
     *  1 4
     *
     */
    template <typename Graph>
    bool snap_parser(Graph& graph, const std::string& str) {
      if (str.empty()) return true;
      else if (str[0] == '#') {
        std::cout << str << std::endl;
      } else {
        graph_vid_t source, target;
        char* targetptr;
        source = strtoul(str.c_str(), &targetptr, 10);
        if (targetptr == NULL) return false;
        target = strtoul(targetptr, NULL, 10);
        if(source != target) graph.add_edge(source, target);
      }
      return true;
    } // end of snap parser

    /**
     * \brief Parse files in the standard tsv format
     *
     * This is identical to the SNAP format but does not allow comments.
     *
     */
    template <typename Graph>
    bool tsv_parser(Graph& graph, const std::string& str) {
      if (str.empty()) return true;
      size_t source, target;
      char* targetptr;
      source = strtoul(str.c_str(), &targetptr, 10);
      if (targetptr == NULL) return false;
      target = strtoul(targetptr, NULL, 10);
      if(source != target) graph.add_edge(source, target);
      return true;
    } // end of tsv parser



#if defined(__cplusplus) && __cplusplus >= 201103L
    // The spirit parser seems to have issues when compiling under
    // C++11. Temporary workaround with a hard coded parser. TOFIX
    template <typename Graph>
    bool adj_parser(Graph& graph, const std::string& line) {
      // If the line is empty simply skip it
      if(line.empty()) return true;
      std::stringstream strm(line);
      graph_vid_t source; 
      size_t n;
      strm >> source;
      if (strm.fail()) return false;
      strm >> n;
      if (strm.fail()) return true;

      size_t nadded = 0;
      while (strm.good()) {
        graph_vid_t target;
        strm >> target;
        if (strm.fail()) break;
        if (source != target) graph.add_edge(source, target);
        ++nadded;
      } 

      if (n != nadded) return false;
      return true;
    } // end of adj parser

#else

    template <typename Graph>
    bool adj_parser(Graph& graph, const std::string& line) {
      // If the line is empty simply skip it
      if(line.empty()) return true;
      // We use the boost spirit parser which requires (too) many separate
      // namespaces so to make things clear we shorten them here.
      namespace qi = boost::spirit::qi;
      namespace ascii = boost::spirit::ascii;
      namespace phoenix = boost::phoenix;
      graph_vid_t source(-1);
      graph_vid_t ntargets(-1);
      std::vector<graph_vid_t> targets;
      const bool success = qi::phrase_parse
        (line.begin(), line.end(),       
         //  Begin grammar
         (
          qi::ulong_[phoenix::ref(source) = qi::_1] >> -qi::char_(",") >>
          qi::ulong_[phoenix::ref(ntargets) = qi::_1] >> -qi::char_(",") >>
          *(qi::ulong_[phoenix::push_back(phoenix::ref(targets), qi::_1)] % -qi::char_(","))
          )
         ,
         //  End grammar
         ascii::space); 
      // Test to see if the boost parser was able to parse the line
      if(!success || ntargets != targets.size()) {
        logstream(LOG_ERROR) << "Parse error in vertex prior parser." << std::endl;
        return false;
      }
      for(size_t i = 0; i < targets.size(); ++i) {
        if(source != targets[i]) graph.add_edge(source, targets[i]);
      }
      return true;
    } // end of adj parser
#endif
  } // namespace builtin_parsers
} // namespace graphlab

#endif
