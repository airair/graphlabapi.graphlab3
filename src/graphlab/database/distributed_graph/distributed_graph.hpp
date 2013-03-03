#ifndef GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_HPP
#define GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_HPP
#include <vector>
#include <algorithm>
#include <functional>
#include <fstream>
#include <sstream>

#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/database/graph_sharding_constraint.hpp>
#include <graphlab/database/query_messages.hpp>
#include <graphlab/database/distributed_graph/idistributed_graph.hpp>
#include <graphlab/database/distributed_graph/builtin_parsers.hpp>
#include <graphlab/database/distributed_graph/distributed_graph_vertex.hpp>
#include <graphlab/util/fs_util.hpp>
#include <fault/query_object_client.hpp>


#include <boost/functional.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/filesystem.hpp>
#include <boost/concept/requires.hpp>

#include <graphlab/macros_def.hpp>

namespace graphlab {
  /**
   * \ingroup group_graph_database
   * An shared memory implementation of a graph database.  
   * This class implements the <code>graph_database</code> interface
   * as a shared memory instance.
   */
  class distributed_graph :public idistributed_graph{
    // Schema for vertex and edge datatypes
    std::vector<graph_field> vertex_fields;
    std::vector<graph_field> edge_fields;

    // Dependencies between shards
    sharding_constraint sharding_graph;

    // Hash function for vertex id.
    boost::hash<graph_vid_t> vidhash;

    // Hash function for edge id.
    boost::hash<std::pair<graph_vid_t, graph_vid_t> > edge_hash;


    typedef libfault::query_object_client::query_result query_result;

    // Graph Database Server object, will be replace to comm object in the future
    QueryMessages messages;

    // local server holding all shards. Used for testing only.
    graph_database_server* server; 

    // the actual query object which connects to the graph db server.
    libfault::query_object_client* qoclient;

    // a list of shard server names.
    std::vector<std::string> server_list;

    typedef QueryMessages::vertex_record vertex_record;
    typedef QueryMessages::edge_record edge_record;
    typedef QueryMessages::mirror_record mirror_record;

    typedef boost::function<bool(distributed_graph&, const std::string&,
                                 const std::string&)> line_parser_type;

    std::string query (const std::string& server_name, char* msg, size_t msg_len) {
      if (server != NULL)  {
        std::string reply = server->query(msg, msg_len);
        free(msg);
        return reply;
      } else {
        query_result result = qoclient->query(server_name, msg, msg_len);
        if (result.get_status() != 0) {
          logstream(LOG_WARNING) << "Error: query to " << server_name << " failed." << std::endl;
          return "ERROR";
        } else {
          return result.get_reply();
        }
      }
    }

    std::string update (const std::string& server_name, char* msg, size_t msg_len) {
      if (server != NULL)  {
        std::string reply = server->update(msg, msg_len);
        free(msg);
        return reply;
      } else {
        query_result result = qoclient->update(server_name, msg, msg_len);
        if (result.get_status() != 0) {
          logstream(LOG_WARNING) << "Error: query to " << server_name << " failed." << std::endl;
          return "ERROR";
        } else {
          return result.get_reply();
        }
      }
    }


    void update_async(const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue) {
      if (server != NULL)  {
        std::string reply = server->update(msg, msg_len);
        free(msg);
      } else {
        query_result result = qoclient->update(server_name, msg, msg_len);
        queue.push_back(result);
      }
    }

    void query_async(const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue) {
      if (server != NULL)  {
        std::string reply = server->query(msg, msg_len);
        free(msg);
      } else {
        query_result result = qoclient->query(server_name, msg, msg_len);
        queue.push_back(result);
      }
    }

   public:
    distributed_graph (graph_database_server*  server) : server(server), qoclient(NULL) { 
      int vfield_req_len, efield_req_len, sharding_req_len;
      char* vfieldreq = messages.vfield_request(&vfield_req_len);
      char* efieldreq = messages.efield_request(&efield_req_len);
      char* shardingreq = messages.sharding_graph_request(&sharding_req_len);

      std::string vfieldrep = query("local", vfieldreq, vfield_req_len);
      std::string efieldrep = query("local", efieldreq, efield_req_len);
      std::string shardingrep = query("local", shardingreq, sharding_req_len);

      bool success = true;
      std::string errormsg;
      success &= messages.parse_reply(vfieldrep, vertex_fields, errormsg);
      success &= messages.parse_reply(efieldrep, edge_fields, errormsg);
      success &= messages.parse_reply(shardingrep, sharding_graph, errormsg);
      ASSERT_TRUE(success);

      server_list.push_back("local"); // in local mode, there is only one server

      vertex_buffer.resize(sharding_graph.num_shards());
      edge_buffer.resize(sharding_graph.num_shards());
      mirror_buffer.resize(sharding_graph.num_shards());
    }

    distributed_graph (void* zmq_ctx,
                       std::vector<std::string> zkhosts,
                       std::string& prefix,
                       std::vector<std::string> server_list) : server(NULL), server_list(server_list) {
      qoclient = new libfault::query_object_client(zmq_ctx, zkhosts, prefix);
      ASSERT_GT(server_list.size(), 0);

      int vfield_req_len, efield_req_len, sharding_req_len;
      char* vfieldreq = messages.vfield_request(&vfield_req_len);
      char* efieldreq = messages.efield_request(&efield_req_len);
      char* shardingreq = messages.sharding_graph_request(&sharding_req_len);

      std::string vfieldrep = query(server_list[0], vfieldreq, vfield_req_len);
      std::string efieldrep = query(server_list[0], efieldreq, efield_req_len);
      std::string shardingrep = query(server_list[0], shardingreq, sharding_req_len);

      bool success = true;
      std::string errormsg;
      success &= messages.parse_reply(vfieldrep, vertex_fields, errormsg);
      success &= messages.parse_reply(efieldrep, edge_fields, errormsg);
      success &= messages.parse_reply(shardingrep, sharding_graph, errormsg);
      ASSERT_TRUE(success);

      vertex_buffer.resize(sharding_graph.num_shards());
      edge_buffer.resize(sharding_graph.num_shards());
      mirror_buffer.resize(sharding_graph.num_shards());
    }

    /**
     * Destroy the graph, free all vertex and edge data from memory.
     */
    virtual ~distributed_graph() {
      if (qoclient != NULL) {
        delete qoclient;
      }
    }

    /**
     * Returns the number of vertices in the graph.
     * This may be slow.
     */
    size_t num_vertices() {
      size_t count, ret = 0;
      bool success;
      std::string errormsg;
      for (size_t i = 0; i < server_list.size(); i++) {
        int msg_len;
        char* req = messages.num_verts_request(&msg_len);
        std::string rep = query(server_list[i], req, msg_len);
        success = messages.parse_reply(rep, count, errormsg);
        ASSERT_TRUE(success);
        ret += count;
      }
      return ret;
    };

    /**
     * Returns the number of edges in the graph.
     * This may be slow.
     */
    size_t num_edges() {
      size_t count, ret = 0;
      bool success;
      std::string errormsg;
      for (size_t i = 0; i < server_list.size(); i++) {
        int msg_len;
        char* req = messages.num_edges_request(&msg_len);
        std::string rep = query(server_list[i], req, msg_len);
        messages.parse_reply(rep, count, errormsg);
        ASSERT_TRUE(success);
        ret += count;
      }
      return ret;
    }

    /**
     * Returns the field metadata for the vertices in the graph
     */
    std::vector<graph_field> get_vertex_fields() {
      return vertex_fields;
    };

    /**
     * Returns the field metadata for the edges in the graph
     */
    std::vector<graph_field> get_edge_fields() {
      return edge_fields;
    };

    /**
     * Returns the sharding constraint graph.
     */
    sharding_constraint get_sharding_constraint() {
      return sharding_graph;
    }


    // -------- Fine grained API ------------
    graph_shard_id_t get_master(graph_vid_t vid) {
      return vidhash(vid) % sharding_graph.num_shards(); 
    } 

    /**
     * Returns a graph_vertex object for the queried vid. Returns NULL on failure
     * The vertex data is passed eagerly as a pointer. Adjacency information is passed through the <code>edge_index</code>. 
     * The returned vertex pointer must be freed using free_vertex
     */
    graph_vertex* get_vertex(graph_vid_t vid) {
      int msg_len;
      char* vertex_req = messages.vertex_request(&msg_len, vid);
      std::string vertex_rep = query(find_server(get_master(vid)), vertex_req, msg_len);
      distributed_graph_vertex* ret = new distributed_graph_vertex(this);
      std::string errormsg; 
      if (messages.parse_reply(vertex_rep, *ret, errormsg)) {
        return ret;
      } else {
        delete ret;
        return NULL;
      }
    };

    /**
     * Returns a graph_edge object for quereid eid, and shardid. Returns NULL on failure.
     * The edge data is passed eagerly as a pointer. 
     * The returned edge pointer must be freed using free_edge.
     */
    graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid) {
      return NULL;
    }

    /**
     *  Finds a vertex using an indexed integer field. Returns the vertex IDs
     *  in out_vids corresponding to the vertices where the integer field 
     *  identified by fieldpos has the specified value.
     *  Return true on success, and false on failure.
     */
    bool find_vertex(size_t fieldpos,
                     graph_int_t value, 
                     std::vector<graph_vid_t>* out_vids) {
      // not implemented
      ASSERT_TRUE(false);
      return false;
    };

    /**
     *  Finds a vertex using an indexed string field. Returns the vertex IDs
     *  in out_vids corresponding to the vertices where the string field 
     *  identified  by fieldpos has the specified value.
     *  Return true on success, and false on failure.
     */
    bool find_vertex(size_t fieldpos,
                     graph_string_t value, 
                     std::vector<graph_vid_t>* out_vids) {
      // not implemented
      ASSERT_TRUE(false);
      return false;
    };

    /**
     * Frees a vertex object.
     * The associated data is not freed. 
     */
    void free_vertex(graph_vertex* vertex) {
      // not implemented
      ASSERT_TRUE(false);
    };

    /**
     * Frees a single edge object.
     * The associated data is not freed. 
     */
    void free_edge(graph_edge* edge) {
      // not implemented
      ASSERT_TRUE(false);

    }

    /**
     * Frees a collection of edges. The vector will be cleared on return.
     */
    void free_edge_vector(std::vector<graph_edge*>* edgelist) {
      // not implemented
      ASSERT_TRUE(false);
    }


    //  ------ Coarse Grained API ---------

    /**
     * Returns the total number of shards in the distributed graph 
     */
    size_t num_shards() { return sharding_graph.num_shards(); }

    /**
     * Returns a reference of the shard from storage.
     */
    graph_shard* get_shard(graph_shard_id_t shardid) {
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

    /**
     * Gets the contents of the shard which are adjacent to some other shard.
     * Creats a new shard with only the relevant edges, and no vertices.
     * It makes a copy of the edge data from the original shard, and fills in the <code>shard_impl.edgeid</code>
     * with the index from the original shard.
     * Returns NULL on failure.
     */
    graph_shard* get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                           graph_shard_id_t adjacent_to) {
      int msg_len;
      char* shardreq = messages.shard_content_adj_request(&msg_len, shard_id, adjacent_to);
      std::string server_name = find_server(adjacent_to);
      std::string shardrep = query(server_name, shardreq, msg_len);

      bool success;
      std::string errormsg;
      graph_shard* shard  = new graph_shard();
      success = messages.parse_reply(shardrep, *shard, errormsg);
      if (!success) {
        free_shard(shard);
      }
      return shard;
    }

    /**
     * Frees a shard. Frees all edge and vertex data from the memory. 
     * All pointers to the data in the shard will be invalid. 
     */  
    void free_shard(graph_shard* shard) {
      shard->clear();
      delete(shard);
      shard = NULL;
    }

    /** 
     * Returns a list of shards IDs which adjacent to a given shard id
     */
    void adjacent_shards(graph_shard_id_t shard_id, 
                         std::vector<graph_shard_id_t>* out_adj_shard_ids) { 
      sharding_graph.get_neighbors(shard_id, *out_adj_shard_ids);
    }

    /**
     * Commits all the changes made to the vertex data and edge data 
     * in the shard, resetting all modification flags.
     */
    void commit_shard(graph_shard* shard) {
      ASSERT_TRUE(false);
    }

    /*
     * Map from shardid to shard server name.
     */
    std::string find_server(graph_shard_id_t shardid) {
      return "shard"+boost::lexical_cast<std::string>(find_server_id(shardid));
    }

    /*
     * Map from shardid to shard server index.
     */
    size_t find_server_id(graph_shard_id_t shardid) {
      return shardid % server_list.size();
    }


    // ----------- Ingress API -----------------
    bool add_vertex_now (graph_vid_t vid, graph_row* data=NULL) {
      graph_shard_id_t master = sharding_graph.get_master(vid);
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

    /**
     * Insert an edge from source to target with given value.
     * This will add vertex mirror info to the master shards if they were not added before.
     * The corresponding vertex mirrors and edge index are updated.
     */
    void add_edge_now(graph_vid_t source, graph_vid_t target, graph_row* data=NULL) {
      graph_shard_id_t master = sharding_graph.get_master(source, target);

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

    void add_vertex (graph_vid_t vid, const graph_row* data=NULL) {
      graph_shard_id_t master = sharding_graph.get_master(vid);
      vertex_buffer[master].push_back(vertex_record(vid, data));
      if (vertex_buffer[master].size() > 100000) {
        flush_vertex_buffer(master);
      }
    }

    void add_edge (graph_vid_t source, graph_vid_t target, const graph_row* data=NULL) {
      graph_shard_id_t master = sharding_graph.get_master(source, target);
      edge_buffer[master].push_back(edge_record(source, target, data));

      typedef boost::unordered_set<graph_shard_id_t>::iterator iter_type;
      std::pair<iter_type, bool> result;  

      // if the edge introduces a new mirror
      result = ingress_mirror_table[source].mirrors.insert(master);
      if (result.second) {
        mirror_record& mrec = mirror_buffer[sharding_graph.get_master(source)][source];
        mrec.mirrors.insert(master);
      }

      // if the edge introduces a new mirror
      result = ingress_mirror_table[target].mirrors.insert(master);
      if (result.second) {
        mirror_record& mrec = mirror_buffer[sharding_graph.get_master(target)][target];
        mrec.mirrors.insert(master);
      }

      if (edge_buffer[master].size() > 1000000) {
        flush_edge_buffer(master);
      }
    }

    void flush() {
      for (size_t i = 0; i < vertex_buffer.size(); i++) {
        flush_vertex_buffer(i);
      }
      for (size_t i = 0; i < edge_buffer.size(); i++) {
        flush_edge_buffer(i);
      }
    }

    /**
     *  \brief load a graph with a standard format. Must be called on all 
     *  machines simultaneously.
     * 
     *  The supported graph formats are described in \ref graph_formats.
     */
    void load_format(const std::string& path, const std::string& format) {
      line_parser_type line_parser;
      if (format == "snap") {
        line_parser = builtin_parsers::snap_parser<distributed_graph>;
        load(path, line_parser);
      } else if (format == "adj") {
        line_parser = builtin_parsers::adj_parser<distributed_graph>;
        load(path, line_parser);
      } else if (format == "tsv") {
        line_parser = builtin_parsers::tsv_parser<distributed_graph>;
        load(path, line_parser);
      // } else if (format == "graphjrl") {
      //   line_parser = builtin_parsers::graphjrl_parser<distributed_graph>;
      // //   load(path, line_parser);
      // } else if (format == "bintsv4") {
      //    load_direct(path,&graph_type::load_bintsv4_from_stream);
      // } else if (format == "bin") {
      //    load_binary(path);
      } else {
        logstream(LOG_ERROR)
          << "Unrecognized Format \"" << format << "\"!" << std::endl;
        return;
      }
    } // end of load


    void load(std::string prefix, line_parser_type line_parser) {
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
    void load_from_posixfs(std::string prefix, 
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


   private: 
    bool add_vertex_mirror(graph_vid_t vid, graph_shard_id_t mirror) {
      graph_shard_id_t master = sharding_graph.get_master(vid);
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

    void flush_vertex_buffer(size_t i) {
      if (vertex_buffer[i].size() == 0)
        return;

      // send vertices in batch
      int len;
      char* msg = messages.batch_add_vertex_request(&len, i, vertex_buffer[i]);
      // query_result rep = qoclient->update(find_server(i), msg, len);
      // ingress_reply.push_back(rep); 
      update_async(find_server(i), msg, len, ingress_reply);
      foreach(vertex_record& vrec, vertex_buffer[i]) {
        if (vrec.data != NULL) {
          delete vrec.data;
        }
      }
      vertex_buffer[i].clear();
    }

    void flush_edge_buffer(size_t i) {
      // send edges in batch 
      if (edge_buffer[i].size() == 0)
        return;

      int len;
      char* msg = messages.batch_add_edge_request(&len, i, edge_buffer[i]);
      // query_result rep = qoclient->update(find_server(i), msg, len);
      // ingress_reply.push_back(rep);
      update_async(find_server(i), msg, len, ingress_reply);

      // send mirrors in batch
      if (mirror_buffer[i].size() > 0)  {
        int len;
        char* msg = messages.batch_add_vertex_mirror_request(&len, i, mirror_buffer[i]);
        // query_result rep = qoclient->update(find_server(i), msg, len);
        // ingress_reply.push_back(rep);
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
      flush();
      return true;
    } // end of load from stream


    void clear_buffers() {
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

    // Buffer of length num_shards()
    std::vector< std::vector<vertex_record> > vertex_buffer;
    std::vector< std::vector<edge_record> > edge_buffer;
    std::vector< boost::unordered_map<graph_vid_t, mirror_record> > mirror_buffer;
    std::vector< query_result > ingress_reply;

    boost::unordered_map<graph_vid_t, mirror_record>  ingress_mirror_table;

    friend class distributed_graph_vertex;
  };
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif

  // void graph_database_server::get_vertex_global_adj(iarchive& iarc, oarchive& oarc) {
  //   graph_vid_t vid;
  //   bool get_in, get_out, prefetch_data;
  //   iarc >> vid >> get_in >> get_out >> prefetch_data;
  //   graph_vertex* v = this->database->get_vertex(vid);
  //   std::vector<query_result> replies;
  //   if (v == NULL) {
  //     std::string msg= (std::string("Fail to get vertex id = ")
  //                       + boost::lexical_cast<std::string>(vid)
  //                       + std::string(". Vid does not exist."));
  //     oarc << false << msg;
  //   } else {
  //     typedef std::vector<graph_edge*> edge_vec;
  //     std::vector<graph_shard_id_t>& shards = v->get_shard_list();
  //     // allocate result spaces for each shard 
  //     std::vector<edge_vec> _inadjs(shards.size());
  //     std::vector<edge_vec> _outadjs(shards.size());

  //     for (size_t i = 0; i < shards.size(); i++) {
  //       if (database->get_shard(shards[i]) != NULL) {
  //         // local shard
  //         edge_vec* out_inadj = get_in ? &(_inadjs[i]) : NULL;
  //         edge_vec* out_outadj = get_out ? &(_outadjs[i]) : NULL;
  //         database->get_adj_list(vid, shards[i], prefetch_data, out_inadj, out_outadj);
  //       } else {
  //         // remote shard
  //         int msg_len;
  //         char* query_msg = messages.vertex_adj_request(&msg_len, vid, shards[i],
  //                                                       getin, getout, prefetch_data);
  //         replies.push_back(qoclient->query(find_server_name(shards[i], query_msg, msg_len)));
  //       }
  //     }

  //     bool success = true;
  //     // processing results
  //     for (size_t i = 0; i < replies.size(); i++) {
  //       if (replies[i].get_status() == 0) {
  //         std::string reply  = replies.get_reply();
  //         iarchive iarc(reply.c_str(), reply.length());
  //       } else {

  //         success = false;
  //         break;
  //       }
  //     }

  //     // getting replies
  //     if (success) {

  //     } else {

  //     }
  //     // finalize 
  //     if (get_in) {
  //       for (size_t i = 0; i < _inadjs.size(); i++) {
  //         database.free_edge_vector(&_inadjs[i]);
  //       }
  //     }
  //     if (get_out) {
  //       for (size_t i = 0; i < _outadjs.size(); i++) {
  //         database.free_edge_vector(&_outadjs[i]);
  //       }
  //     }
  //   }

  //   for (size_t i = 0; i < replies.size()) {

  //   }

  //   // merge results

  //   database->free_vertex(v);
  // }
