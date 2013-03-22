#include <graphlab/database/client/graph_loader.hpp>
#include <graphlab/database/client/graphdb_client.hpp>
#include <graphlab/util/fs_util.hpp>

#include <boost/functional.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/filesystem.hpp>
#include <boost/concept/requires.hpp>

#include <vector>
#include <algorithm>
#include <functional>
#include <fstream>
#include <sstream>

namespace graphlab {
  graph_loader::graph_loader(graphdb_client* client) : client(client) { }

  /**
   *  \brief load a graph with a standard format. Must be called on all 
   *  machines simultaneously.
   * 
   *  The supported graph formats are described in \ref graph_formats.
   */
  void graph_loader::load_format(const std::string& path, const std::string& format) {
    line_parser_type line_parser;
    if (format == "snap") {
      line_parser = builtin_parsers::snap_parser<graph_loader>;
      load(path, line_parser);
    } else if (format == "adj") {
      line_parser = builtin_parsers::adj_parser<graph_loader>;
      load(path, line_parser);
    } else if (format == "tsv") {
      line_parser = builtin_parsers::tsv_parser<graph_loader>;
      load(path, line_parser);
    } else {
      logstream(LOG_ERROR)
          << "Unrecognized Format \"" << format << "\"!" << std::endl;
      return;
    }
  } // end of load

  void graph_loader::load(std::string prefix, line_parser_type line_parser) {
    if (prefix.length() == 0) return;
    load_from_posixfs(prefix, line_parser);
  } // end of load

  /**
   *  \brief Load a graph from a collection of files in stored on
   *  the filesystem using the user defined line parser. Like 
   *  \ref load(const std::string& path, line_parser_type line_parser) 
   *  but only loads from the filesystem. 
   */
  void graph_loader::load_from_posixfs(std::string prefix, line_parser_type line_parser) {
    std::string directory_name; std::string original_path(prefix);
    boost::filesystem::path path(prefix);
    std::string search_prefix;
    if (boost::filesystem::is_directory(path)) {
      // if this is a directory
      // force a "/" at the end of the path
      // make sure to check that the path is non-empty. (you do not
      // want to make the empty path "" the root path "/" )
      directory_name = path.native();
    }
    else {
      directory_name = path.parent_path().native();
      search_prefix = path.filename().native();
      directory_name = (directory_name.empty() ? "." : directory_name);
    }
    std::vector<std::string> graph_files;
    fs_util::list_files_with_prefix(directory_name, search_prefix, graph_files);
    if (graph_files.size() == 0) {
      logstream(LOG_WARNING) << "No files found matching " << original_path << std::endl;
    }
    for(size_t i = 0; i < graph_files.size(); ++i) {
      logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
      // is it a gzip file ?
      const bool gzip = boost::ends_with(graph_files[i], ".gz");
      // open the stream
      std::ifstream in_file(graph_files[i].c_str(), 
                            std::ios_base::in | std::ios_base::binary);
      // attach gzip if the file is gzip
      boost::iostreams::filtering_stream<boost::iostreams::input> fin;  
      // Using gzip filter
      if (gzip) fin.push(boost::iostreams::gzip_decompressor());
      fin.push(in_file);
      const bool success = load_from_stream(graph_files[i], fin, line_parser);
      if(!success) {
        logstream(LOG_FATAL) 
            << "\n\tError parsing file: " << graph_files[i] << std::endl;
      }
      fin.pop();
      if (gzip) fin.pop();
    }
  } // end of load from posixfs

  void graph_loader::add_edge(graph_vid_t source, graph_vid_t target) {
    graph_row row;
    row._is_vertex = false;
    client->add_edge(source, target, row);
  } 
} // end of namespace
