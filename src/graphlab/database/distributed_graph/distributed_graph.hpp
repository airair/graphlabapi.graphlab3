#ifndef GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_HPP
#define GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_HPP
#include <vector>
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
#include <fault/query_object_client.hpp>
#include <graphlab/macros_def.hpp>

namespace graphlab {
/**
 * \ingroup group_graph_database
 * An shared memory implementation of a graph database.  
 * This class implements the <code>graph_database</code> interface
 * as a shared memory instance.
 */
class distributed_graph {
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


  std::string query (const std::string& server_name, char* msg, size_t msg_len) {
    if (server != NULL)  {
      return server->query(msg, msg_len);
    } else {
      query_result result = qoclient->query(server_name, msg, msg_len);
      if (result.get_status() != 0) {
        logstream(LOG_WARNING) << "Error: query to " << server_name << " failed.";
        return "ERROR";
      } else {
        return result.get_reply();
      }
    }
  }

  std::string update (const std::string& server_name, char* msg, size_t msg_len) {
    if (server != NULL)  {
      return server->update(msg, msg_len);
    } else {
      query_result result = qoclient->update(server_name, msg, msg_len);
      if (result.get_status() != 0) {
        logstream(LOG_WARNING) << "Error: query to " << server_name << " failed.";
        return "ERROR";
      } else {
        return result.get_reply();
      }
    }
  }

 public:
  distributed_graph (graph_database_server*  server) : server(server), qoclient(NULL) { 
    int vfield_req_len, efield_req_len, sharding_req_len;
    char* vfieldreq = messages.vfield_request(&vfield_req_len);
    char* efieldreq = messages.efield_request(&efield_req_len);
    char* shardingreq = messages.sharding_graph_request(&sharding_req_len);
    
    bool success;
    std::string vfieldrep = query("local", vfieldreq, vfield_req_len);
    std::string efieldrep = query("local", efieldreq, efield_req_len);
    std::string shardingrep = query("local", shardingreq, sharding_req_len);

    iarchive iarc_vfields(vfieldrep.c_str(), vfieldrep.length());
    iarc_vfields >> success >> vertex_fields;
    ASSERT_TRUE(success);

    iarchive iarc_efields(efieldrep.c_str(), efieldrep.length()); 
    iarc_efields >> success >> edge_fields;
    ASSERT_TRUE(success);

    iarchive iarc_sharding_graph(shardingrep.c_str(), shardingrep.length());
    iarc_sharding_graph >> success >> sharding_graph;
    ASSERT_TRUE(success);

    server_list.push_back("local"); // in local mode, there is only one server
  }

  distributed_graph (void* zmq_ctx,
                     std::vector<std::string> zkhosts,
                     std::string& prefix,
                     std::vector<std::string> server_list) : server(NULL), server_list(server_list) {
    qoclient = new libfault::query_object_client(zmq_ctx, zkhosts, prefix);
    ASSERT_GT(server_list.size(), 0);
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
  uint64_t num_vertices() {
    size_t count, ret = 0;
    bool success;
    for (size_t i = 0; i < server_list.size(); i++) {
      int msg_len;
      char* req = messages.num_verts_request(&msg_len);
      std::string rep = query(server_list[i], req, msg_len);
      iarchive iarc(rep.c_str(), rep.length());
      iarc >> success;
      ASSERT_TRUE(success);
      iarc >> count;
      ret += count;
    }
    return ret;
  };
  
  /**
   * Returns the number of edges in the graph.
   * This may be slow.
   */
  uint64_t num_edges() {
    size_t count, ret = 0;
    bool success;
    for (size_t i = 0; i < server_list.size(); i++) {
      int msg_len;
      char* req = messages.num_edges_request(&msg_len);
      std::string rep = query(server_list[i], req, msg_len);
      iarchive iarc(rep.c_str(), rep.length());
      iarc >> success;
      ASSERT_TRUE(success);
      iarc >> count;
      ret += count;
    }
    return ret;
  }


  /**
   * Returns the field metadata for the vertices in the graph
   */
  const std::vector<graph_field>& get_vertex_fields() {
    return vertex_fields;
  };

  /**
   * Returns the field metadata for the edges in the graph
   */
  const std::vector<graph_field>& get_edge_fields() {
    return edge_fields;
  };

  /**
   * Returns the sharding constraint graph.
   */
  const sharding_constraint& get_sharding_constraint() {
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
    return NULL;
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

      // Map from shardid to server name: Assuming using mod for now.
      int msg_len;
      char* shardreq = messages.shard_request(&msg_len, shardid);
      std::string server_name = find_server(shardid);
      std::string shardrep = query(server_name, shardreq, msg_len);
      iarchive iarc_shard(shardrep.c_str(), shardrep.length()); 
      bool success;
      iarc_shard >> success;
      if (success) {
        graph_shard* shard = new graph_shard(); 
        iarc_shard >> *shard;
        return shard;
      } else {
        std::string errormsg;
        iarc_shard >> errormsg;
        logstream(LOG_WARNING) << errormsg;
        return NULL;
      }
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
      iarchive iarc_shard(shardrep.c_str(), shardrep.length()); 
      bool success;
      iarc_shard >> success;
      if (success) {
        graph_shard* shard = new graph_shard(); 
        iarc_shard >> shard;
        return shard;
      } else {
        std::string errormsg;
        iarc_shard >> errormsg;
        logstream(LOG_WARNING) << errormsg;
        return NULL;
      }
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
    size_t id = shardid % server_list.size();
    return "shard"+boost::lexical_cast<std::string>(id);
  }

// ----------- Modification API -----------------
  /*
   * Insert the vertex v into a shard = hash(v) as master
   * The insertion updates the global <code>vertex_store</code> as well as the master shard.
   * The corresponding <code>vid2master</code> and <code>vertex_index</code> are updated.
   * Return false if v is already inserted.
   */
  bool add_vertex(graph_vid_t vid, graph_row* data=NULL) {
    graph_shard_id_t master = sharding_graph.get_master(vid);
    int msg_len;
    char* req = messages.add_vertex_request(&msg_len, vid, master, data);

    std::string server_name = find_server(master);
    std::string rep = update(server_name, req, msg_len);
    iarchive iarc(rep.c_str(), rep.length());

    bool success;
    iarc >> success;
    if (!success) {
      std::string msg;
      iarc >> msg;
      logstream(LOG_WARNING) << msg;
    }
    return success;
  }

  /**
   * Insert an edge from source to target with given value.
   * This will add vertex mirror info to the master shards if they were not added before.
   * The corresponding vertex mirrors and edge index are updated.
   */
  void add_edge(graph_vid_t source, graph_vid_t target, graph_row* data=NULL) {
    graph_shard_id_t master = sharding_graph.get_master(source, target);

    int msg_len;
    char* req = messages.add_edge_request(&msg_len, source, target, master, data);

    std::string server_name = find_server(master);
    std::string rep = update(server_name, req, msg_len);
    iarchive iarc(rep.c_str(), rep.length());

    bool success;
    iarc >> success;
    if (!success) {
      std::string msg;
      iarc >> msg;
      logstream(LOG_WARNING) << msg;
    } else {
      success &= add_vertex_mirror(source, master);
      success &= add_vertex_mirror(target, master);
      ASSERT_TRUE(success);
    }
  }

 private: 
  bool add_vertex_mirror(graph_vid_t vid, graph_shard_id_t mirror) {
    graph_shard_id_t master = sharding_graph.get_master(vid);
    int msg_len;
    char* req = messages.add_vertex_mirror_request(&msg_len,
                                                   vid,
                                                   master,
                                                   mirror);

    std::string server_name = find_server(master);
    std::string rep = update(server_name, req, msg_len);
    iarchive iarc(rep.c_str(), rep.length());
    bool success;
    iarc >> success;
    if (!success) {
      std::string msg;
      iarc >> msg;
      logstream(LOG_WARNING) << msg;
    }
    return success;
  }
};
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif
