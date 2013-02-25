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

  // Graph Database Server object, will be replace to comm object in the future
  QueryMessages messages;
  graph_database_server* server;

 public:
  distributed_graph (graph_database_server*  server) : server(server) { 
    std::string vfieldreq = messages.vfield_request();
    std::string efieldreq = messages.efield_request();
    std::string shardingreq = messages.sharding_graph_request();
    
    bool success;
    std::string vfieldrep = server->query(vfieldreq.c_str(), vfieldreq.length());
    std::string efieldrep = server->query(efieldreq.c_str(), efieldreq.length());
    std::string shardingrep = server->query(shardingreq.c_str(), shardingreq.length());

    iarchive iarc_vfields(vfieldrep.c_str(), vfieldrep.length());
    iarc_vfields >> success >> vertex_fields;
    ASSERT_TRUE(success);

    iarchive iarc_efields(efieldrep.c_str(), efieldrep.length()); 
    iarc_efields >> success >> edge_fields;
    ASSERT_TRUE(success);

    iarchive iarc_sharding_graph(shardingrep.c_str(), shardingrep.length());
    iarc_sharding_graph >> success >> sharding_graph;
    ASSERT_TRUE(success);
  }

   /**
    * Destroy the graph, free all vertex and edge data from memory.
    */
   virtual ~distributed_graph() {
   }

  /**
   * Returns the number of vertices in the graph.
   * This may be slow.
   */
  uint64_t num_vertices() {
    size_t ret = 0;
    std::string req = messages.num_verts_request();
    std::string rep = server->query(req.c_str(), req.length());
    bool success;
    iarchive iarc(rep.c_str(), rep.length());
    iarc >> success >> ret;
    ASSERT_TRUE(success);
    return ret;
  };
  
  /**
   * Returns the number of edges in the graph.
   * This may be slow.
   */
  uint64_t num_edges() {
    size_t ret = 0;
    std::string req = messages.num_edges_request();
    std::string rep = server->query(req.c_str(), req.length());
    bool success;
    iarchive iarc(rep.c_str(), rep.length());
    iarc >> success >> ret;
    ASSERT_TRUE(success);
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
      std::string shardreq = messages.shard_request(shardid);
      std::string shardrep = server->query(shardreq.c_str(), shardreq.length());
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
      std::string shardreq = messages.shard_content_adj_request(shard_id, adjacent_to);
      std::string shardrep = server->query(shardreq.c_str(), shardreq.length());
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
    // not implemented
    // for (size_t i = 0; i < shard->num_vertices(); i++) {
    //   graph_row* row = shard->vertex_data(i);
    //   for (size_t j = 0; j < row->num_fields(); j++) {
    //     graph_value* val = row->get_field(j);
    //     if (val->get_modified()) {
    //       val->post_commit_state();
    //     }
    //   }
    // }

    ASSERT_TRUE(false);
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
    std::string req = messages.add_vertex_request(vid,
                                                         master,
                                                         data);

    std::string rep = server->update(req.c_str(), req.length());
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

    std::string req = messages.add_edge_request(source,
                                                       target,
                                                       master,
                                                       data);

    std::string rep = server->update(req.c_str(), req.length());
    iarchive iarc(rep.c_str(), rep.length());

    bool success;
    iarc >> success;
    if (!success) {
      std::string msg;
      iarc >> msg;
      logstream(LOG_WARNING) << msg;
    } else {
      success &=  add_vertex_mirror(source, master);
      success &=  add_vertex_mirror(target, master);
      ASSERT_TRUE(success);
    }
  }

 private: 
  bool add_vertex_mirror(graph_vid_t vid, graph_shard_id_t mirror) {
    graph_shard_id_t master = sharding_graph.get_master(vid);
    std::string req = messages.add_vertex_mirror_request(vid,
                                                       master,
                                                       mirror);

    std::string rep = server->update(req.c_str(), req.length());
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
