#ifndef GRAPHLAB_DATABASE_GRAPH_CLIENT_HPP
#define GRAPHLAB_DATABASE_GRAPH_CLIENT_HPP
#include <graphlab/database/basic_types.hpp>
#include <fault/query_object_client.hpp>

namespace graphlab {
  class graph_row;
  class graph_edge;
  class graph_vertex;
  class graph_shard;
  class graph_field;
  class graph_shard_manager;
  /**
   * \ingroup group_graph_database
   * Interface of a graph query_client. Provides functionality
   * for issue query/update request to graph_database_server.
   **/
  class graph_client {

   public:
     typedef libfault::query_object_client::query_result query_result;
  
    virtual ~graph_client() {};
    // ------------ Query Interafce --------------------
    virtual std::string query (const std::string& server_name, char* msg, size_t msg_len) = 0;
    virtual std::string update (const std::string& server_name, char* msg, size_t msg_len) = 0;
    virtual void update_async (const std::string& server_name, char* msg, size_t msg_len,
                               std::vector<query_result>& queue) = 0;
    virtual void query_async (const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue) = 0;
    virtual void update_all (char* msg, size_t msg_len,
                             std::vector<query_result>& reply_queue) = 0;
    virtual void query_all(char* msg, size_t msg_len,
                             std::vector<query_result>& reply_queue) = 0;
    virtual std::string find_server(graph_shard_id_t shardid)  = 0;

    // ------------ Basic info query API --------------------
    virtual size_t num_vertices() = 0;
    virtual size_t num_edges() = 0;
    virtual std::vector<graph_field> get_vertex_fields() = 0;
    virtual std::vector<graph_field> get_edge_fields() = 0;
    virtual const graph_shard_manager& get_shard_manager() = 0;

    // ------------ Fine grained API --------------------
    virtual graph_vertex* get_vertex(graph_vid_t vid)  = 0;
    virtual std::vector<graph_vertex*> get_vertex_adj_to_shard(
        graph_shard_id_t shard_from, graph_shard_id_t shard_to)  = 0;
    virtual std::vector< std::vector<graph_vertex*> > batch_get_vertices(
        const std::vector<graph_vid_t>& vids)  = 0;
    virtual graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid) = 0 ;
    virtual void free_vertex(graph_vertex* vertex) = 0;
    virtual void free_vertex_vector(std::vector<graph_vertex*>& vertexlist) = 0;
    virtual void free_edge(graph_edge* edge) = 0;
    virtual void free_edge_vector(std::vector<graph_edge*>& edgelist)  = 0;

    // ------------ Corse grained API --------------------
    virtual size_t num_shards() = 0;
    virtual graph_shard* get_shard(graph_shard_id_t shardid) = 0;
    virtual graph_shard* get_shard_contents_adj_to(const std::vector<graph_vid_t>& vids,
                                       graph_shard_id_t adjacent_to) = 0;
    virtual void free_shard(graph_shard* shard) = 0;
    virtual void adjacent_shards(graph_shard_id_t shard_id,
                                 std::vector<graph_shard_id_t>* out_adj_shard_ids) = 0;
    virtual void commit_shard(graph_shard* shard) = 0;

    // ----------------- Dynamic Field API -------------
    virtual void add_vertex_field(graph_field& field) = 0;
    virtual void add_edge_field(graph_field& field) = 0;
    virtual void remove_vertex_field(size_t fieldpos) = 0;
    virtual void remove_edge_field(size_t fieldpos) = 0;

    // ----------------- Ingress API --------------------
    virtual bool add_vertex_now (graph_vid_t vid, graph_row* data=NULL) = 0; 
    virtual void add_edge_now(graph_vid_t source, graph_vid_t target, graph_row* data=NULL) = 0;
    virtual void add_vertex (graph_vid_t vid, graph_row* data=NULL) = 0;
    virtual void add_edge (graph_vid_t source, graph_vid_t target, graph_row* data=NULL) = 0;
    virtual void flush() = 0;
    virtual void load_format(const std::string& path, const std::string& format) = 0;
  };
}
#endif
