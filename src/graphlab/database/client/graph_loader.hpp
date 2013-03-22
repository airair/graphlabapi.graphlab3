#ifndef GRAPHLAB_DATABASE_GRAPH_LOADER_HPP
#define GRAPHLAB_DATABASE_GRAPH_LOADER_HPP

#include <graphlab/database/client/builtin_parsers.hpp>
#include <boost/functional.hpp>
#include <string>

namespace graphlab {
  class graphdb_client;

  class graph_loader {
   public:
     typedef boost::function<bool(graph_loader&, const std::string&, const std::string&)> line_parser_type;

   public:
     graph_loader(graphdb_client* client);

     void load_format(const std::string& path, const std::string& format);

     void add_edge(graph_vid_t source, graph_vid_t target);

   private:
     void load(std::string prefix, line_parser_type line_parser);

     void load_from_posixfs(std::string prefix, line_parser_type line_parser);

     /**
       \internal
       This internal function is used to load a single line from an input stream
       */
     template<typename Fstream>
     bool load_from_stream(std::string filename, Fstream& fin, 
                           line_parser_type& line_parser) {
       size_t linecount = 0;
       timer ti; ti.start();
       while(fin.good() && !fin.eof()) {
         std::string line;
         std::getline(fin, line);
         if(line.empty()) continue;
         if(fin.fail()) break;
         const bool success = line_parser(*this, filename, line);
         if (!success) {
           logstream(LOG_WARNING) 
               << "Error parsing line " << linecount << " in "
               << filename << ": " << std::endl
               << "\t\"" << line << "\"" << std::endl;  
           return false;
         }
         ++linecount;      
         if (ti.current_time() > 5.0 || (linecount % 100000 == 0)) {
           logstream(LOG_INFO) << linecount << " Lines read" << std::endl;
           ti.start();
         }
       }
       return true;
     }

  private:
     graphdb_client* client;
  };
}

#endif
