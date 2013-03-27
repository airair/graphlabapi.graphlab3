#ifndef GRAPHLAB_DATABASE_GRAPH_LOADER_HPP
#define GRAPHLAB_DATABASE_GRAPH_LOADER_HPP
#include <graphlab/database/client/ingress/ingress_worker.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/parallel/thread_pool.hpp>

#include <boost/bind.hpp>
#include <string>

namespace graphlab {
  class graphdb_client;
  class graph_loader {
   public:
     graph_loader(graphdb_client* client, size_t max_buffer = 500000);

     void load_from_posixfs(std::string prefix, const std::string& format);

   private:
     /**
       \internal
       This internal function is used to load a single line from an input stream
       */
     template<typename Fstream>
         bool load_from_stream(const std::string& filename, Fstream& fin, 
                               const std::string& format) {
           size_t linecount = 0;
           std::vector< std::vector<std::string> > line_buffers;
           std::vector<std::string> buffer;
           buffer.reserve(max_buffer);

           timer ti; ti.start();
           while(fin.good() && !fin.eof()) {
             std::string line;
             std::getline(fin, line);
             if(line.empty()) continue;
             if(fin.fail()) break;

             buffer.push_back(line);
             ++linecount;      

             if (buffer.size() == max_buffer) {
               // add a worker
               line_buffers.push_back(std::vector<std::string>());
               line_buffers.back().swap(buffer);
               pool.launch(boost::bind(&graph_loader::process_lines, this, line_buffers.back(), format, filename, linecount-buffer.size()+1));
             }
           }

           // add the last worker
           line_buffers.push_back(std::vector<std::string>());
           line_buffers.back().swap(buffer);
           pool.launch(boost::bind(&graph_loader::process_lines, this, line_buffers.back(), format, filename, linecount-buffer.size()+1));
           pool.join();
           line_buffers.clear();
           return true;
         }

   private:
     void process_lines(std::vector<std::string>& lines,
                        const std::string& format,
                        const std::string& fname,
                        size_t line_count_begin) {
       ingress_worker worker(client, format);
       worker.process_lines(lines, fname, line_count_begin);
     }

     graphdb_client* client;
     size_t max_buffer;
     thread_pool pool;
  };
}
#endif
