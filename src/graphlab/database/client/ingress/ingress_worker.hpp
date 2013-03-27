#ifndef GRAPHLAB_DATABASE_INGRESS_WORKER_HPP
#define GRAPHLAB_DATABASE_INGRESS_WORKER_HPP
#include <graphlab/database/graph_database.hpp>
#include <vector>
#include <boost/functional.hpp>

namespace graphlab {
  class graphdb_client;

  class ingress_worker {

   typedef graph_database::edge_insert_descriptor edge_insert_descriptor;

   typedef boost::function<bool(ingress_worker&, const std::string&)> line_parser_type;
   public:
     ingress_worker(graphdb_client* client,
                    const std::string& format);

     void process_lines(std::vector<std::string>& lines,
                        const std::string& fname,
                        size_t line_count_begin);

     void add_edge(graph_vid_t source, graph_vid_t dest);

   private:
     void flush();

   private:
     graphdb_client* client;
     line_parser_type line_parser;
     std::vector<edge_insert_descriptor> edge_ingress_buffer; 
  };
}
#endif
