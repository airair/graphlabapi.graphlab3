#include <graphlab/database/client/distributed_graph_client.hpp>
#include <graphlab/database/client/builtin_parsers.hpp>
#include <graphlab/database/client/graph_vertex_remote.hpp>
#include <graphlab/database/client/graph_edge_remote.hpp>
#include <graphlab/database/server/graph_database_server.hpp>
#include <graphlab/database/graph_database.hpp>

#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
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

#include <graphlab/macros_def.hpp>
namespace graphlab {
  // ------------------ Constructors ---------------------------
  distributed_graph_client::distributed_graph_client (graph_database_server* server)
      : server(server), qoclient(NULL), shard_manager(server->get_database()->num_shards()) { 
        vertex_buffer.resize(num_shards());
        edge_buffer.resize(num_shards());
        mirror_buffer.resize(num_shards());
      }

  distributed_graph_client::distributed_graph_client (std::vector<std::string> zkhosts,
                                                      std::string& prefix,
                                                      const graph_shard_manager& shard_manager,
                                                      const boost::unordered_map<graph_shard_id_t, std::string>& _shard2server)
      : server(NULL), shard_manager(shard_manager), shard2server(_shard2server) {

        void* zmq_ctx = zmq_ctx_new();
        qoclient = new libfault::query_object_client(zmq_ctx, zkhosts, prefix);
        vertex_buffer.resize(num_shards());
        edge_buffer.resize(num_shards());
        mirror_buffer.resize(num_shards());

        std::set<std::string> unique_server;
        typedef boost::unordered_map<graph_shard_id_t, std::string>::iterator iterator;
        for (iterator it = shard2server.begin(); it != shard2server.end(); ++it) {
          unique_server.insert(it->second);
        }
        server_list.insert(server_list.end(), unique_server.begin(), unique_server.end());
      }

  distributed_graph_client::~distributed_graph_client() {
    if (qoclient != NULL) {
      delete qoclient;
    }
  }

  // ------------------ Query Graph Info API ---------------------------
  size_t distributed_graph_client::num_vertices() {
    size_t count, ret = 0;
    bool success;
    std::string errormsg;
    for (size_t i = 0; i < num_shards(); i++) {
      int msg_len;
      char* req = messages.num_verts_request(&msg_len, i);
      std::string rep = query(find_server(i), req, msg_len);
      success = messages.parse_reply(rep, count, errormsg);
      ASSERT_TRUE(success);
      ret += count;
    }
    return ret;
  };

  size_t distributed_graph_client::num_edges() {
    size_t count, ret = 0;
    bool success;
    std::string errormsg;
    for (size_t i = 0; i < num_shards(); i++) {
      int msg_len;
      char* req = messages.num_edges_request(&msg_len, i);
      std::string rep = query(find_server(i), req, msg_len);
      messages.parse_reply(rep, count, errormsg);
      ASSERT_TRUE(success);
      ret += count;
    }
    return ret;
  }

  std::vector<graph_field> distributed_graph_client::get_vertex_fields() {
    int msg_len;
    char* request = messages.vfield_request(&msg_len);
    std::string reply = query(server_list[0], request, msg_len);
    std::vector<graph_field> fields;
    std::string errormsg;
    messages.parse_reply(reply, fields,  errormsg);
    return fields;
  }

  std::vector<graph_field> distributed_graph_client::get_edge_fields() {
    int msg_len;
    char* request = messages.efield_request(&msg_len);
    std::string reply = query(server_list[0], request, msg_len);
    std::vector<graph_field> fields;
    std::string errormsg;
    messages.parse_reply(reply, fields,  errormsg);
    return fields;
  }

  // ------------------ Fine Grained API ---------------------------
  graph_vertex* distributed_graph_client::get_vertex(graph_vid_t vid) {
    int msg_len;
    graph_shard_id_t master = shard_manager.get_master(vid);
    char* vertex_req = messages.vertex_request(&msg_len, vid, master);
    std::string vertex_rep = query(find_server(master), vertex_req, msg_len);
    graph_vertex_remote* ret = new graph_vertex_remote(this);
    std::string errormsg; 
    if (messages.parse_reply(vertex_rep, *ret, errormsg)) {
      return ret;
    } else {
      delete ret;
      return NULL;
    }
  };


  std::vector< std::vector<graph_vertex*> > distributed_graph_client::batch_get_vertices(const std::vector<graph_vid_t>& vids) {
    std::vector< std::vector<graph_vid_t> > vid_per_shard(num_shards());
    std::vector< std::pair<char*, int> > message_per_shard(num_shards());
    std::vector< query_result > reply_queue;
    std::vector< std::vector<graph_vertex*> > ret (num_shards());

    // group vids by server
    for (size_t i = 0; i < vids.size(); i++) {
      vid_per_shard[shard_manager.get_master(vids[i])].push_back(vids[i]);
    }

    // group request/replies by server
    for (size_t i = 0; i < num_shards(); i++) {
      std::pair<char*, int>& msg_pair = message_per_shard[i];
      msg_pair.first = messages.batch_vertex_request(&(msg_pair.second), vid_per_shard[i], i);

      query_async(find_server(i), msg_pair.first, msg_pair.second, reply_queue);
    }

    // check and parse replies 
    for (size_t i = 0; i < reply_queue.size(); i++) {
      query_result result = reply_queue[i];
      if (result.get_status() != 0) {
        logstream(LOG_ERROR) << error_messages.server_not_reachable(find_server(i)) << std::endl;
      } else {
        graph_vertex_remote* head;
        size_t size;
        std::vector<std::string> errormsgs;
        messages.parse_batch_reply(result.get_reply(), &head, &size, errormsgs);

        ret[i].resize(size);
        for (size_t j = 0; j < size; j++) {
          head[j].graph = this;
          ret[i][j] = (head+j);
        }
      }
    }
    return ret;
  };

  std::vector< graph_vertex* > distributed_graph_client::get_vertex_adj_to_shard(graph_shard_id_t shard_from, graph_shard_id_t shard_to) {
    int len;
    char* request = messages.vertex_adj_to_shard(&len, shard_from, shard_to);
    std::string reply = query(find_server(shard_from), request, len);

    graph_vertex_remote* head;
    size_t size;
    std::vector<std::string> errormsgs;
    bool success = messages.parse_batch_reply(reply, &head, &size, errormsgs); 
    std::vector<graph_vertex*> ret;
    if (success) {
      ret.resize(size);
      for (size_t i = 0; i < size; i++) {
        head[i].graph = this;
        ret[i] = (graph_vertex*)(head + i);
      }
    }
    return ret;
  }

  graph_edge* distributed_graph_client::get_edge(graph_eid_t eid, graph_shard_id_t shardid) {
    int msg_len;
    char* request = messages.edge_request(&msg_len, eid, shardid);
    std::string reply = query(find_server(shardid), request, msg_len);
    graph_edge_remote* ret = new graph_edge_remote(this);
    std::string errormsg; 
    if (messages.parse_reply(reply, *ret, errormsg)) {
      return ret;
    } else {
      delete ret;
      return NULL;
    }
  }

  void distributed_graph_client::free_vertex(graph_vertex* vertex) { delete (graph_vertex_remote*)vertex; }

  void distributed_graph_client::free_vertex_vector (std::vector<graph_vertex*>& vertexlist) {
    if (vertexlist.size() == 0) {
      return;
    }
    graph_vertex_remote* head = (graph_vertex_remote*)vertexlist[0];
    delete[] head;
    vertexlist.clear();
  }

  void distributed_graph_client::free_edge(graph_edge* edge) { delete (graph_edge_remote*)edge; }

  void distributed_graph_client::free_edge_vector(std::vector<graph_edge*>& edgelist) {
    if (edgelist.size() == 0)
      return;
    graph_edge_remote* head = (graph_edge_remote*)edgelist[0];
    delete[] head;
    edgelist.clear();
  }




  // ---------------------- Coarse Grained API ----------------------------
  graph_shard* distributed_graph_client::get_shard(graph_shard_id_t shardid) {
    int msg_len;
    char* shardreq = messages.shard_request(&msg_len, shardid);
    std::string server_name = find_server(shardid);
    std::string shardrep = query(server_name, shardreq, msg_len);
    bool success;
    std::string errormsg;
    graph_shard* shard  = new graph_shard();
    success = messages.parse_reply(shardrep, *shard, errormsg);
    if (!success) {
      free_shard(shard);
      return NULL;
    }
    return shard;
  }

  graph_shard* distributed_graph_client::get_shard_contents_adj_to(const std::vector<graph_vid_t>& vids,
                                                                   graph_shard_id_t adjacent_to) {
    int msg_len;
    char* shardreq = messages.shard_content_adj_request(&msg_len, vids, adjacent_to);
    std::string server_name = find_server(adjacent_to);
    std::string shardrep = query(server_name, shardreq, msg_len);

    bool success;
    std::string errormsg;
    graph_shard* shard  = new graph_shard();
    success = messages.parse_reply(shardrep, *shard, errormsg);
    if (!success) {
      delete shard;
      shard = NULL;
    }
    return shard;
  }

  void distributed_graph_client::commit_shard(graph_shard* shard) {
    ASSERT_TRUE(false);
  }

  // ----------------- Dynamic Field API -------------
  void distributed_graph_client::add_field(graph_field& field, bool is_vertex) {
    int msg_len;
    char* request = messages.add_field(&msg_len, is_vertex, field);
    std::vector<query_result> reply_queue;
    update_all(request, msg_len, reply_queue);
    std::string errormsg;
    for (size_t i = 0; i < reply_queue.size(); i++) {
      if (reply_queue[i].get_status() != 0) {
        logstream(LOG_ERROR) << error_messages.server_not_reachable(server_list[i]);
      } else {
        messages.parse_reply(reply_queue[i].get_reply(), errormsg);
      }
    }
  }

  void distributed_graph_client::remove_field(size_t fieldpos, bool is_vertex) {
    int msg_len;
    char* request = messages.remove_field(&msg_len, is_vertex, fieldpos);
    std::vector<query_result> reply_queue;
    update_all(request, msg_len, reply_queue);
    std::string errormsg;
    for (size_t i = 0; i < reply_queue.size(); i++) {
      if (reply_queue[i].get_status() != 0) {
        logstream(LOG_ERROR) << error_messages.server_not_reachable(server_list[i]);
      } else {
        messages.parse_reply(reply_queue[i].get_reply(), errormsg);
      }
    }
  }

  void distributed_graph_client::add_vertex_field(graph_field& field) {
    add_field(field, true);
  }

  void distributed_graph_client::add_edge_field(graph_field& field) {
    add_field(field, false);
  }

  void distributed_graph_client::remove_vertex_field(size_t fieldpos) {
    remove_field(fieldpos, true);
  }

  void distributed_graph_client::remove_edge_field(size_t fieldpos) {
    remove_field(fieldpos, false);
  }

  // ----------- Ingress API -----------------

  bool distributed_graph_client::add_vertex_now (graph_vid_t vid, graph_row* data) {
    graph_shard_id_t master = shard_manager.get_master(vid);
    int msg_len;
    vertex_record vrecord(vid, data);
    char* req = messages.add_vertex_request(&msg_len, master, vrecord);
    std::string server_name = find_server(master);
    std::string rep = update(server_name, req, msg_len);

    std::string errormsg;
    bool success = messages.parse_reply(rep, errormsg);

    if (data != NULL) {
      delete data;
    }
    return success;
  }

  void distributed_graph_client::add_edge_now(graph_vid_t source, graph_vid_t target, graph_row* data) {
    graph_shard_id_t master = shard_manager.get_master(source, target);

    int msg_len;
    edge_record erecord(source, target, data);
    char* req = messages.add_edge_request(&msg_len, master, erecord);

    std::string server_name = find_server(master);
    std::string rep = update(server_name, req, msg_len);

    std::string errormsg;
    bool success = messages.parse_reply(rep, errormsg);
    success &= add_vertex_mirror(source, master);
    success &= add_vertex_mirror(target, master);
    ASSERT_TRUE(success);

    if (data != NULL) {
      delete data;
    }
  }


  void distributed_graph_client::add_vertex (graph_vid_t vid, graph_row* data) {
    graph_shard_id_t master = shard_manager.get_master(vid);
    vertex_buffer[master].push_back(vertex_record(vid, data));
    if (vertex_buffer[master].size() > 1000000) {
      flush_vertex_buffer(master);
    }
  }

  void distributed_graph_client::add_edge (graph_vid_t source, graph_vid_t target, graph_row* data) {
    graph_shard_id_t master = shard_manager.get_master(source, target);
    edge_buffer[master].push_back(edge_record(source, target, data));

    typedef boost::unordered_set<graph_shard_id_t>::iterator iter_type;
    std::pair<iter_type, bool> result;  

    // if the edge introduces a new mirror
    result = ingress_mirror_table[source].mirrors.insert(master);
    if (result.second) {
      mirror_record& mrec = mirror_buffer[shard_manager.get_master(source)][source];
      mrec.mirrors.insert(master);
    }

    // if the edge introduces a new mirror
    result = ingress_mirror_table[target].mirrors.insert(master);
    if (result.second) {
      mirror_record& mrec = mirror_buffer[shard_manager.get_master(target)][target];
      mrec.mirrors.insert(master);
    }

    if (edge_buffer[master].size() > 1000000) {
      flush_edge_buffer(master);
    }
  }


  void distributed_graph_client::flush() {
    for (size_t i = 0; i < vertex_buffer.size(); i++) {
      flush_vertex_buffer(i);
    }
    for (size_t i = 0; i < edge_buffer.size(); i++) {
      flush_edge_buffer(i);
    }
  }

  void distributed_graph_client::flush_vertex_buffer (size_t i) {
    if (vertex_buffer[i].size() == 0)
      return;
    // send vertices in batch
    int len;
    char* msg = messages.batch_add_vertex_request(&len, i, vertex_buffer[i]);
    update_async(find_server(i), msg, len, ingress_reply);
    foreach(vertex_record& vrec, vertex_buffer[i]) {
      if (vrec.data != NULL) {
        delete vrec.data;
      }
    }
    vertex_buffer[i].clear();
  }

  void distributed_graph_client::flush_edge_buffer(size_t i) {
    // send edges in batch 
    if (edge_buffer[i].size() == 0)
      return;

    int len;
    char* msg = messages.batch_add_edge_request(&len, i, edge_buffer[i]);
    update_async(find_server(i), msg, len, ingress_reply);

    // send mirrors in batch
    if (mirror_buffer[i].size() > 0)  {
      int len;
      char* msg = messages.batch_add_vertex_mirror_request(&len, i, mirror_buffer[i]);
      update_async(find_server(i), msg, len, ingress_reply);
    }

    // clear buffer
    foreach(edge_record& erec, edge_buffer[i]) {
      if (erec.data != NULL) {
        delete erec.data;
      }
    }
    edge_buffer[i].clear();
    mirror_buffer[i].clear();
  }


  bool distributed_graph_client::add_vertex_mirror 
      (graph_vid_t vid, graph_shard_id_t mirror) {
        graph_shard_id_t master = shard_manager.get_master(vid);
        int msg_len;
        mirror_record mirror_record; 
        mirror_record.vid = vid;
        mirror_record.mirrors.insert(mirror);
        char* req = messages.add_vertex_mirror_request(&msg_len,
                                                       master,
                                                       mirror_record);

        std::string server_name = find_server(master);
        std::string rep = update(server_name, req, msg_len);
        iarchive iarc(rep.c_str(), rep.length());
        bool success;
        iarc >> success;
        if (!success) {
          std::string msg;
          iarc >> msg;
          logstream(LOG_WARNING) << msg << std::endl;
        }
        return success;
      }

  void distributed_graph_client::clear_buffers() {
    for (size_t i = 0; i < vertex_buffer.size(); i++) {
      vertex_buffer[i].clear();
      std::vector<vertex_record>().swap(vertex_buffer[i]);
    }
    for (size_t i = 0; i < edge_buffer.size(); i++) {
      edge_buffer[i].clear();
      std::vector<edge_record>().swap(edge_buffer[i]);
    }
    ingress_mirror_table.clear();
    boost::unordered_map<graph_vid_t, mirror_record>().swap(ingress_mirror_table);
  }


  // -------------------------- Loading API -------------------------
  /**
   *  \brief load a graph with a standard format. Must be called on all 
   *  machines simultaneously.
   * 
   *  The supported graph formats are described in \ref graph_formats.
   */
  void distributed_graph_client::load_format(const std::string& path, const std::string& format) {
    line_parser_type line_parser;
    if (format == "snap") {
      line_parser = builtin_parsers::snap_parser<graph_client>;
      load(path, line_parser);
    } else if (format == "adj") {
      line_parser = builtin_parsers::adj_parser<graph_client>;
      load(path, line_parser);
    } else if (format == "tsv") {
      line_parser = builtin_parsers::tsv_parser<graph_client>;
      load(path, line_parser);
    } else {
      logstream(LOG_ERROR)
          << "Unrecognized Format \"" << format << "\"!" << std::endl;
      return;
    }
  } // end of load


  void distributed_graph_client::load(std::string prefix, line_parser_type line_parser) {
    if (prefix.length() == 0) return;
    load_from_posixfs(prefix, line_parser);
    clear_buffers();
    for (size_t i = 0;  i < ingress_reply.size(); ++i) {
      if (ingress_reply[i].get_status() == 0) {
        std::string reply = ingress_reply[i].get_reply();
        std::string errormsg;
        bool success = messages.parse_reply(reply, errormsg);
        if (!success) {
          break;
        }
      } else {
        logstream(LOG_ERROR) << "Loading failed: Couldn't reach server." << std::endl;
        break;
      }
    }
    ingress_reply.clear();
  } // end of load

  /**
   *  \brief Load a graph from a collection of files in stored on
   *  the filesystem using the user defined line parser. Like 
   *  \ref load(const std::string& path, line_parser_type line_parser) 
   *  but only loads from the filesystem. 
   */
  void distributed_graph_client::load_from_posixfs(std::string prefix, 
                                                   line_parser_type line_parser) {
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


  // ------------------------- Raw Query API ---------------------

  /**
   * Query server with a query message. Block until getting the reply.
   * This function takes over the msg pointer.
   */
  std::string distributed_graph_client::query (const std::string& server_name, char* msg, size_t msg_len) {
    if (server != NULL)  {
      std::string reply = server->query(msg, msg_len);
      free(msg);
      return reply;
    } else {
      query_result result = qoclient->query(server_name, msg, msg_len);
      if (result.get_status() != 0) {
        std::string errormsg = error_messages.server_not_reachable(server_name); 
        logstream(LOG_WARNING) << errormsg << std::endl;
        ASSERT_TRUE(false);
      } else {
        return result.get_reply();
      }
    }
  }

  /**
   * Query server with a update message. Block until getting the reply.
   * This function takes over the msg pointer.
   */
  std::string distributed_graph_client::update (const std::string& server_name, char* msg, size_t msg_len) {
    if (server != NULL)  {
      std::string reply = server->update(msg, msg_len);
      free(msg);
      return reply;
    } else {
      query_result result = qoclient->update(server_name, msg, msg_len);
      if (result.get_status() != 0) {
        std::string errormsg = error_messages.server_not_reachable(server_name); 
        logstream(LOG_WARNING) << errormsg << std::endl;
        ASSERT_TRUE(false);
      } else {
        return result.get_reply();
      }
    }
  }

  /**
   * Send query message asynchronously. The future reply object is pushed into the given reply queue. 
   * This function takes over the msg pointer.
   */
  void distributed_graph_client::update_async(const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue) {
    if (server != NULL)  {
      std::string reply = server->update(msg, msg_len);
      free(msg);
    } else {
      query_result result = qoclient->update(server_name, msg, msg_len);
      queue.push_back(result);
    }
  }

  /**
   * Send update message asynchronously. The future reply object is pushed into the given reply queue. 
   * This function takes over the msg pointer.
   */
  void distributed_graph_client::query_async(const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue) {
    if (server != NULL)  {
      std::string reply = server->query(msg, msg_len);
      free(msg);
    } else {
      query_result result = qoclient->query(server_name, msg, msg_len);
      queue.push_back(result);
    }
  }


  void distributed_graph_client::update_all(char* msg, size_t msg_len,
                                            std::vector<query_result>& reply_queue) {
    for (size_t i = 0; i < server_list.size(); ++i) {
      char* msg_copy = (char*)malloc(msg_len);
      memcpy(msg_copy, msg, msg_len);
      update_async(server_list[i], msg_copy, msg_len, reply_queue);
    }
    free(msg);
  }

  void distributed_graph_client::query_all(char* msg, size_t msg_len,
                                           std::vector<query_result>& reply_queue) {
    for (size_t i = 0; i < server_list.size(); ++i) {
      char* msg_copy = (char*)malloc(msg_len);
      memcpy(msg_copy, msg, msg_len);
      query_async(server_list[i], msg_copy, msg_len, reply_queue);
    }
    free(msg);
  }
}
#include <graphlab/macros_undef.hpp>
