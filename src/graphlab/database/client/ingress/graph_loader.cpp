#include <graphlab/database/client/ingress/graph_loader.hpp>
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
  graph_loader::graph_loader(graphdb_client* client,
                             size_t max_buffer) : client(client),
    max_buffer(max_buffer), pool(4) { }

  /**
   *  \brief Load a graph from a collection of files in stored on
   *  the filesystem using the user defined line parser. Like 
   *  \ref load(const std::string& path, line_parser_type line_parser) 
   *  but only loads from the filesystem. 
   */
  void graph_loader::load_from_posixfs(std::string prefix, const std::string& format) {
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

    timer ti; ti.start();
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
      const bool success = load_from_stream(graph_files[i], fin, format);
      if(!success) {
        logstream(LOG_FATAL) 
            << "\n\tError parsing file: " << graph_files[i] << std::endl;
      }
      fin.pop();
      if (gzip) fin.pop();
    }
    logstream(LOG_EMPH) << "Finish loading. Total time: " << ti.current_time()
                        << " secs." << std::endl;
  } // end of load from posixfs
} // end of namespace
