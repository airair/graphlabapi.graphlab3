#ifndef GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_CLIENT_HPP
#define GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_CLIENT_HPP
#include <graphlab/database/client/graph_client.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_shard_manager.hpp>
#include <graphlab/database/query_messages.hpp>
#include <graphlab/database/error_messages.hpp>

namespace graphlab {
  class graph_database_server;
  class graph_vertex_remote;
  class graph_edge_remote;
  /**
   * \ingroup group_graph_database
   * Implementation of a distributed graph query client.
   * This client can act in local mode, and distributed mode.
   * In local mode, it takes in a pointer to a graph_database_server and simulates the query.
   * In distributed mode, it uses query_object_client to communicate with servers.
   */
  class distributed_graph_client :public graph_client {

    // Schema for vertex and edge datatypes
    std::vector<graph_field> vertex_fields;
    std::vector<graph_field> edge_fields;

    typedef libfault::query_object_client::query_result query_result;

    // Defines the protocal messages between server and client.
    QueryMessages messages;

    // Defines the error message when query failed.
    ErrorMessages error_messages;

    // local server holding all shards. Used for testing only.
    graph_database_server* server; 

    // the actual query object which connects to the graph db server.
    libfault::query_object_client* qoclient;

    graph_shard_manager shard_manager;

    // Mapping from shard id to server name. 
    boost::unordered_map<graph_shard_id_t, std::string> shard2server;

    // List of unique server name.
    std::vector<std::string> server_list;

    typedef QueryMessages::vertex_record vertex_record;
    typedef QueryMessages::edge_record edge_record;
    typedef QueryMessages::mirror_record mirror_record;

    typedef boost::function<bool(graph_client&, const std::string&,
                                 const std::string&)> line_parser_type;

   public:
    /**
     * Query server with a query message. Block until getting the reply.
     * This function takes over the msg pointer.
     */
    std::string query (const std::string& server_name, char* msg, size_t msg_len);

    /**
     * Query server with a update message. Block until getting the reply.
     * This function takes over the msg pointer.
     */
    std::string update (const std::string& server_name, char* msg, size_t msg_len);

    /**
     * Send query message asynchronously. The future reply object is pushed into the given reply queue. 
     * This function takes over the msg pointer.
     */
    void update_async(const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue);

    /**
     * Send update message asynchronously. The future reply object is pushed into the given reply queue. 
     * This function takes over the msg pointer.
     */
    void query_async(const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue);

    /**
     * Send update message to all servers asynchronously.
     */
    void update_all(char* msg, size_t msg_len, std::vector<query_result>& reply_queue);

    /**
     * Send query message to all servers asynchronously.
     */
    void query_all(char* msg, size_t msg_len, std::vector<query_result>& reply_queue);

   public:
    /**
     * Creates a local simulated distributed_graph client with pointer to a shared memory
     * server.
     */
    distributed_graph_client (graph_database_server*);


    /**
     * Creates a distributed_graph client query_object_client configuration and a server name list.
     */
    distributed_graph_client (std::vector<std::string> zkhosts,
                              std::string& prefix,
                              const graph_shard_manager& shard_manager,
                              const boost::unordered_map<graph_shard_id_t, std::string>& _shard2server);


    /**
     * Destroy the graph, free all vertex and edge data from memory.
     */
    virtual ~distributed_graph_client(); 

    /**
     * Returns the number of vertices in the graph.
     * This may be slow.
     */
    size_t num_vertices();

    /**
     * Returns the number of edges in the graph.
     * This may be slow.
     */
    size_t num_edges();

    /**
     * Returns the field metadata for the vertices in the graph
     */
    inline std::vector<graph_field> get_vertex_fields() {
      return vertex_fields;
    };

    /**
     * Returns the field metadata for the edges in the graph
     */
    inline std::vector<graph_field> get_edge_fields() {
      return edge_fields;
    };

    /**
     * Returns the sharding constraint graph.
     */
    inline const graph_shard_manager& get_shard_manager() {
      return shard_manager;
    }


    // -------- Fine grained API ------------
    /**
     * Returns a graph_vertex object for the queried vid. Returns NULL on failure
     * Returns NULL on failure.
     * The returned vertex pointer must be freed using free_vertex
     */
    graph_vertex* get_vertex(graph_vid_t vid);

    /**
     * Returns a vector of graph_vertex list for the queried batch vids. The size of the returned vector is equal to the number of the servers. Returns NULL on failure
     * Returns NULL on failure.
     * The returned vertex vector must be freed using free_vertex_vector
     */
    std::vector< std::vector<graph_vertex*> > batch_get_vertices(const std::vector<graph_vid_t>& vids);


    std::vector< graph_vertex* > get_vertex_adj_to_shard(graph_shard_id_t shard_from, graph_shard_id_t shard_to); 

    /**
     * Returns a graph_edge object for quereid eid, and shardid. Returns NULL on failure.
     * The returned edge pointer must be freed using free_edge.
     */
    graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid);

    /**
     *  Finds a vertex using an indexed integer field. Returns the vertex IDs
     *  in out_vids corresponding to the vertices where the integer field 
     *  identified by fieldpos has the specified value.
     *  Return true on success, and false on failure.
     */
    inline bool find_vertex(size_t fieldpos,
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
    inline bool find_vertex(size_t fieldpos,
                            graph_string_t value, 
                            std::vector<graph_vid_t>* out_vids) {
      // not implemented
      ASSERT_TRUE(false);
      return false;
    };

    /**
     * Frees a vertex object.
     */
    void free_vertex(graph_vertex* vertex);

    /**
     * Frees a collection of vertices. The vector will be cleared on return. 
     */
    void free_vertex_vector (std::vector<graph_vertex*>& vertexlist);

    /**
     * Frees a single edge object.
     */
    inline void free_edge(graph_edge* edge); 

    /**
     * Frees a collection of edges. The vector will be cleared on return.
     */
    void free_edge_vector(std::vector<graph_edge*>& edgelist);


    //  ------ Coarse Grained API ---------

    /**
     * Returns the total number of shards in the distributed graph 
     */
    inline size_t num_shards() { return shard_manager.num_shards(); }

    /**
     * Returns a graph_shard object for the query shardid. Returns NULL on failure.
     * The returned shard pointer must be freed using free_shard.
     */
    graph_shard* get_shard(graph_shard_id_t shardid);

    /**
     * Gets the ontents of the shard which are adjacent to a list of vertices.
     * Creates a new shard with only the relevant edges, and no vertices.
     * Returns NULL on failure.
     */
    graph_shard* get_shard_contents_adj_to(const std::vector<graph_vid_t>& vids,
                                           graph_shard_id_t adjacent_to);

    /**
     * Frees a shard. Frees all edge and vertex data from the memory. 
     * All pointers to the data in the shard will be invalid. 
     */  
    inline void free_shard(graph_shard* shard) {
      delete(shard);
    }

    /** 
     * Returns a list of shards IDs which adjacent to a given shard id
     */
    inline void adjacent_shards(graph_shard_id_t shard_id, 
                                std::vector<graph_shard_id_t>* out_adj_shard_ids) { 
      shard_manager.get_neighbors(shard_id, *out_adj_shard_ids);
    }

    /**
     * Commits all the changes made to the vertex data and edge data 
     * in the shard, resetting all modification flags.
     */
    void commit_shard(graph_shard* shard);

    /*
     * Map from shardid to shard server name.
     */
    inline std::string find_server(graph_shard_id_t shardid) {
      return shard2server[shardid];
    }

  // ----------------- Dynamic Field API -------------
    void add_vertex_field(graph_field& field);

    void add_edge_field(graph_field& field);

    void remove_vertex_field(size_t fieldpos);

    void remove_edge_field(size_t fieldpos);

    // ----------- Ingress API -----------------
    /**
     * Insert a vertex wth given value.
     * This call block until getting the reply from server.
     */
    bool add_vertex_now (graph_vid_t vid, graph_row* data=NULL);

    /**
     * Insert an edge from source to target with given value.
     * This will add vertex mirror info to the master shards if they were not added before.
     * The corresponding vertex mirrors and edge index are updated.
     * This call block until getting the reply from server.
     */
    void add_edge_now(graph_vid_t source, graph_vid_t target, graph_row* data=NULL);

    /**
     * Insert a vertex asynchronously. This call will buffer the insertion with buffersize = 1M.
     * Use flush_vertex_buffer() to send out the buffered request.
     * The future reply will be pushed into ingress_reply.
     */
    void add_vertex (graph_vid_t vid, graph_row* data=NULL);

    /**
     * Insert an edge asynchronously. This call will buffer the insertion with buffersize = 1M.
     * Use flush_edge_buffer() to send out the buffered request.
     * The future reply will be pushed into ingress_reply.
     */
    void add_edge (graph_vid_t source, graph_vid_t target, graph_row* data=NULL);

    /**
     * Flush all buffered requests.
     */
    void flush();

    /**
     *  \brief load a graph with a standard format. Must be called on all 
     *  machines simultaneously.
     * 
     *  The supported graph formats are described in \ref graph_formats.
     */
    void load_format(const std::string& path, const std::string& format);

    void load(std::string prefix, line_parser_type line_parser);

    /**
     *  \brief Load a graph from a collection of files in stored on
     *  the filesystem using the user defined line parser. Like 
     *  \ref load(const std::string& path, line_parser_type line_parser) 
     *  but only loads from the filesystem. 
     */
    void load_from_posixfs(std::string prefix, line_parser_type line_parser);

   private: 
    /**
     * Insert a vertex mirror info.
     */
    bool add_vertex_mirror(graph_vid_t vid, graph_shard_id_t mirror);

    /**
     * Send out the buffered request for vertex insertion.
     */
    void flush_vertex_buffer(size_t i);

    /**
     * Send out the buffered request for edge insertion.
     */
    void flush_edge_buffer(size_t i);

    /**
     * Clear all ingress bufferes.
     */
    void clear_buffers();

    /**
     * Request to a vertex or edge field.
     */
    void add_field(graph_field& field, bool is_vertex);

    /**
     * Request to remove a vertex or edge field.
     */
    void remove_field(size_t fieldpos, bool is_vertex);

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


    // Buffer of length num_shards()
    std::vector< std::vector<vertex_record> > vertex_buffer;
    std::vector< std::vector<edge_record> > edge_buffer;
    std::vector< boost::unordered_map<graph_vid_t, mirror_record> > mirror_buffer;
    std::vector< query_result > ingress_reply;

    boost::unordered_map<graph_vid_t, mirror_record>  ingress_mirror_table;

    friend class graph_vertex_remote;
    friend class graph_edge_remote;
  };
} // namespace graphlab
#endif
