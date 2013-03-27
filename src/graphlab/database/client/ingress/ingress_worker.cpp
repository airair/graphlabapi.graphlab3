#include<graphlab/database/client/ingress/ingress_worker.hpp>
#include<graphlab/database/client/ingress/builtin_parsers.hpp>
#include<graphlab/database/client/graphdb_client.hpp>
#include<graphlab/logger/assertions.hpp>

namespace graphlab {
  ingress_worker::ingress_worker(graphdb_client* _client, 
                                 const std::string& format) : client(_client) { 
    if (format == "snap") {
      line_parser = builtin_parsers::snap_parser<ingress_worker>;
    } else if (format == "adj") {
      line_parser = builtin_parsers::adj_parser<ingress_worker>;
    } else if (format == "tsv") {
      line_parser = builtin_parsers::tsv_parser<ingress_worker>;
    } else {
      logstream(LOG_ERROR)
          << "Unrecognized Format \"" << format << "\"!" << std::endl;
    }
  }

  void ingress_worker::add_edge(graph_vid_t source, graph_vid_t dest) {
    graph_row row;
    row._is_vertex = false;
    edge_insert_descriptor e;
    e.src = source; 
    e.dest = dest;
    e.data = row;
    edge_ingress_buffer.push_back(e);
  }

  void ingress_worker::flush() {
    logstream(LOG_EMPH) << "Flush ... " << std::endl;
    std::vector<int> errorcodes;
    bool success = client->add_edges(edge_ingress_buffer, errorcodes);
    // TODO: add error handling....
    ASSERT_TRUE(success);
    edge_ingress_buffer.clear();
  }

  void ingress_worker::process_lines(std::vector<std::string>& lines,
                                     const std::string& filename,
                                     size_t line_count_begin) {
    edge_ingress_buffer.reserve(lines.size());
    for (size_t i = 0; i < lines.size(); ++i) {
      bool success = line_parser(*this, lines[i]);
      if (!success) {
        logstream(LOG_WARNING) 
            << "Error parsing line " << (i + line_count_begin) << " in "
            << filename << ": " << std::endl
            << "\t\"" << lines[i] << "\"" << std::endl;  
      }
    }
    flush();
    lines.clear();
  }
}
