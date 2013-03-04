#ifndef GRAPHLAB_DATABASE_IDISTRIBUTED_GRAPH_HPP
#define GRAPHLAB_DATABASE_IDISTRIBUTED_GRAPH_HPP
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
  class idistributed_graph {

   public:
     typedef libfault::query_object_client::query_result query_result;
  
    virtual ~idistributed_graph() {};
    // ------------ Query Interafce --------------------
    virtual std::string query (const std::string& server_name, char* msg, size_t msg_len) = 0;
    virtual std::string update (const std::string& server_name, char* msg, size_t msg_len) = 0;
    virtual void update_async (const std::string& server_name, char* msg, size_t msg_len,
                               std::vector<query_result>& queue) = 0;
    virtual void query_async (const std::string& server_name, char* msg, size_t msg_len, std::vector<query_result>& queue) = 0;
    virtual std::string find_server(graph_shard_id_t shardid)  = 0;

    // ------------ Basic info query API --------------------
    virtual size_t num_vertices() = 0;
    virtual size_t num_edges() = 0;
    virtual std::vector<graph_field> get_vertex_fields() = 0;
    virtual std::vector<graph_field> get_edge_fields() = 0;
    virtual sharding_constraint get_sharding_constraint() = 0;

    // ------------ Fine grained API --------------------
    virtual graph_shard_id_t get_master(graph_vid_t vid) = 0;
    virtual graph_vertex* get_vertex(graph_vid_t vid)  = 0;
    virtual graph_edge* get_edge(graph_eid_t eid, graph_shard_id_t shardid) = 0 ;
    virtual void free_vertex(graph_vertex* vertex) = 0;
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


    // ----------------- Ingress API --------------------
    virtual bool add_vertex_now (graph_vid_t vid, graph_row* data=NULL) = 0; 
    virtual void add_edge_now(graph_vid_t source, graph_vid_t target, graph_row* data=NULL) = 0;
    virtual void add_vertex (graph_vid_t vid, const graph_row* data=NULL) = 0;
    virtual void add_edge (graph_vid_t source, graph_vid_t target, const graph_row* data=NULL) = 0;
    virtual void flush() = 0;
    virtual void load_format(const std::string& path, const std::string& format) = 0;
  };
}
#include<graphlab/macros_undef.hpp>
#endif
