#ifndef GRAPHLAB_DATABASE_QUERY_MESSAGES_HPP
#define GRAPHLAB_DATABASE_QUERY_MESSAGES_HPP
#include<graphlab/database/basic_types.hpp>
#include<graphlab/database/graph_field.hpp>
#include<graphlab/database/graph_row.hpp>
#include<fault/query_object_client.hpp>
#include<graphlab/macros_def.hpp>

namespace graphlab {
  /**
   * This class defines the communication protocol between <code>graph_database_server</code>
   * and the <code>distributed_graph</code> client.
   */
  class QueryMessages {
   public:
    typedef libfault::query_object_client::query_result query_result;

   public:
    /**
     * Struct holding information of a vertex during ingress. 
     */
     struct vertex_record {
       graph_vid_t vid;
       const graph_row* data;
       vertex_record(graph_vid_t vid, const graph_row* data) : vid(vid), data(data) { }
     };

    /**
     * Struct holding information of an edge during ingress. 
     */
     struct edge_record {
       graph_vid_t source;
       graph_vid_t target;
       const graph_row* data;
       edge_record(graph_vid_t source, graph_vid_t target, const graph_row* data = NULL) :
           source(source), target(target), data(data) { }
     };

    /**
     * Struct holding information of (vertex, mirrors) pair during ingress. 
     */
     struct mirror_record {
       graph_vid_t vid;
       boost::unordered_set<graph_shard_id_t> mirrors;
       mirror_record() {};
     };

     inline static char* make_message(int* msg_len, std::vector<std::string> msg) {
       oarchive oarc;
       for (size_t i = 0; i < msg.size(); i++) {
         oarc << msg[i];
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

   public:
     // ------------- Query requests  ----------------
     ///  returns a message for querying the vertex field meta data
     inline char* vfield_request(int* msg_len) {
       oarchive oarc;
       oarc << std::string("g_vertex_fields_meta");
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the edge field meta data
     inline char* efield_request(int* msg_len) {
       oarchive oarc;
       oarc << std::string("g_edge_fields_meta");
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the sharding constraint graph 
     inline char* sharding_graph_request(int* msg_len) {
       oarchive oarc;
       oarc << std::string("g_sharding_graph");
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the entire vertex data of vid 
     inline char* vertex_row_request(int* msg_len, graph_vid_t vid, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_vertex_row") << vid << shardid;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying a vertex 
     inline char* vertex_request(int* msg_len, graph_vid_t vid, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_vertex") << vid << shardid; 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the a batch of vertex data of vid 
     inline char* batch_vertex_request(int* msg_len, const std::vector<graph_vid_t>& vids, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_batch_vertex") << vids << shardid;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the vertices adjacent to a shard
     inline char* vertex_adj_to_shard(int* msg_len,
                               graph_shard_id_t shard_from,
                               graph_shard_id_t shard_to) {
       oarchive oarc;
       oarc << std::string("g_vertex_shard_adj") << shard_from << shard_to;
       *msg_len=oarc.off;
       return oarc.buf;
     }


     ///  returns a message for querying the entire vertex data of vid 
     inline char* edge_row_request(int* msg_len, graph_eid_t eid, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_edge_row") << eid << shardid;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying an edge 
     inline char* edge_request(int* msg_len, graph_vid_t eid,
                          graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_edge") << eid << shardid; 
       *msg_len=oarc.off;
       return oarc.buf;
     }


     ///  returns a message for querying a shard 
     inline char* shard_request(int* msg_len, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_shard") << shardid;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying a shard content adjacent to list of vertices
     inline char* shard_content_adj_request (int* msg_len,
                                      const std::vector<graph_vid_t>& vids,
                                      graph_shard_id_t to) {
       oarchive oarc;
       oarc << std::string("g_shard_adj") << vids << to;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the adjacent edges of a vertex
     inline char* vertex_adj_request(int* msg_len,
                              graph_vid_t vid,
                              graph_shard_id_t shardid,
                              bool getin,
                              bool getout) {
       oarchive oarc;
       oarc << std::string("g_vertex_adj") << vid << shardid << getin << getout;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the number of shards (on the target server) 
     inline char* num_shards_request (int* msg_len) {
       oarchive oarc;
       oarc << std::string("g_num_shards"); 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the number of vertices (on the target server) 
     inline char* num_verts_request (int* msg_len, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_num_vertices") << shardid; 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for querying the number of edges (on the target server) 
     inline char* num_edges_request (int* msg_len, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("g_num_edges") << shardid; 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     // ------------- Update request ------------------
     /*
      * Returns a message for updateing the vertex data.
      * The message includes the modified fields of the vertex data. 
      * Currently the message sends the new data (not delta).
      */
     inline char* update_vertex_request(int* msg_len, 
                                 graph_vid_t vid,
                                 graph_shard_id_t shardid,
                                 const std::vector<size_t>& modified_fields,
                                 graph_row* data) {
       oarchive oarc;
       oarc << std::string("s_vertex_row") << vid << shardid;
       oarc << modified_fields.size(); 
       for (size_t i = 0; i < modified_fields.size(); i++) {
         graph_value* val = data->get_field(modified_fields[i]);
         oarc << modified_fields[i] << val->data_length();
         if (val->get_use_delta_commit()) {
           oarc << true;
           switch (val->type())  {
            case INT_TYPE: oarc << (val->_data.int_value-val->_old.int_value); break;
            case DOUBLE_TYPE: oarc << (val->_data.double_value-val->_old.double_value); break;
            default: ASSERT_TRUE(false);
           }
         } else {
           oarc << false;
           oarc.write((char*)val->get_raw_pointer(), val->data_length());
         }
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     /*
      * Returns a message for updating the edge data.
      * The message includes the modified fields of the edge data. 
      * Currently the message sends the new data (not delta).
      * TODO: check delta commit
      */
     inline char* update_edge_request(int* msg_len, 
                                 graph_eid_t eid,
                                 graph_shard_id_t shardid,
                                 const std::vector<size_t>& modified_fields,
                                 graph_row* data) {
       oarchive oarc;
       oarc << std::string("s_edge_row") << eid << shardid;
       oarc << modified_fields.size(); 
       for (size_t i = 0; i < modified_fields.size(); i++) {
         graph_value* val = data->get_field(modified_fields[i]);
         oarc << modified_fields[i] << val->data_length();
         oarc.write((char*)val->get_raw_pointer(), val->data_length());
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     // ------------- Dynamic Field Request --------------
     /// returns a message for adding a field to vertex or edge
     inline char* add_field(int* msg_len,
                                   bool is_vertex,
                                   graph_field& field) {
       oarchive oarc;
       oarc << std::string("gc_add_field") << is_vertex << field;
       *msg_len = oarc.off;
       return oarc.buf;
     }

     /// returns a message for removing a field to vertex or edge
     inline char* remove_field(int* msg_len,
                               bool is_vertex,
                               size_t fieldpos) {
       oarchive oarc;
       oarc << std::string("gc_remove_field") << is_vertex << fieldpos;
       *msg_len = oarc.off;
       return oarc.buf;
     }

     // ------------- Ingress requests  ----------------
     ///  returns a message for adding the a vertex
     inline char* add_vertex_request(int* msg_len,
                              graph_shard_id_t shardid,
                              vertex_record& vrecord) {
       oarchive oarc;
       oarc << std::string("gc_add_vertex") << vrecord.vid << shardid; 
       if (vrecord.data!= NULL) {
         oarc << true << *(vrecord.data);
       } else {
         oarc << false;
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for adding the a list of vertices 
     inline char* batch_add_vertex_request(int* msg_len,
                                    graph_shard_id_t shardid,
                                    std::vector<vertex_record>& vrecords) {
       ASSERT_TRUE(vrecords.size() > 0);
       oarchive oarc;
       oarc << std::string("gc_batch_add_vertex") << shardid << vrecords.size(); 
       foreach(vertex_record& vrec, vrecords) {
         oarc << vrec.vid;
         if (vrec.data!= NULL) {
           oarc << true << *(vrec.data);
         } else {
           oarc << false;
         }
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for adding the an edge 
     inline char* add_edge_request(int* msg_len,
                            graph_shard_id_t shardid,
                            edge_record& erecord) {
       oarchive oarc;
       oarc << std::string("gc_add_edge") << erecord.source << erecord.target << shardid; 
       if (erecord.data != NULL) {
         oarc << true << *(erecord.data);
       } else {
         oarc << false;
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for adding the a list of edges 
     inline char* batch_add_edge_request(int* msg_len,
                                  graph_shard_id_t shardid,
                                  std::vector<edge_record>& erecords) {
       ASSERT_TRUE(erecords.size() > 0);
       oarchive oarc;
       oarc << std::string("gc_batch_add_edge") << shardid << erecords.size();
       foreach(edge_record& erec, erecords) {
         oarc << erec.source << erec.target;
         if (erec.data != NULL) {
           oarc << true << *(erec.data);
         } else {
           oarc << false;
         }
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for adding a vertex mirror 
     inline char* add_vertex_mirror_request(int* msg_len,
                                     graph_shard_id_t shardid,
                                     mirror_record& mrecord){
       oarchive oarc;
       oarc << std::string("gc_add_vertex_mirror") << mrecord.vid << shardid;
       oarc << mrecord.mirrors.size();
       foreach(graph_shard_id_t mirror, mrecord.mirrors) {
         oarc << mirror;
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     ///  returns a message for adding a list of vertex mirror 
     inline char* batch_add_vertex_mirror_request(int* msg_len,
                                           graph_shard_id_t shardid,
                                           boost::unordered_map<graph_vid_t, mirror_record>& mrecords) {
       ASSERT_TRUE(mrecords.size() > 0);
       oarchive oarc;
       oarc << std::string("gc_batch_add_vertex_mirror") << shardid << mrecords.size();

       typedef boost::unordered_map<graph_vid_t, mirror_record> maptype;
       for (maptype::iterator it = mrecords.begin(); it != mrecords.end(); ++it) {
         graph_vid_t vid = it->first;
         size_t num_mirrors = it->second.mirrors.size();
         oarc << vid << num_mirrors;
         foreach (graph_shard_id_t mirror, it->second.mirrors) {
           oarc << mirror;
         }
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }


     // --------------------------- Reply parsers ---------------------------
     template<typename T>
     /**
      * Parse a reply with single return content serialized into the out variable.
      */
     bool parse_reply(std::string reply, T& out, std::string& errormsg) {
       iarchive iarc(reply.c_str(), reply.length());
       bool success;
       iarc >> success;
       if (!success) {
         iarc >> errormsg;
         logstream(LOG_ERROR) <<  errormsg << std::endl;
       } else {
         iarc >> out;
       }
       return success;
     }

     template<typename T>
     /**
      * Parse a reply with batch return content into the out variable.
      */
     bool parse_batch_reply(std::string reply,
                            T** out, size_t* size, std::vector<std::string>& errormsgs) {
       *out = NULL;
       *size = 0;

       iarchive iarc(reply.c_str(), reply.length());
       bool success;
       size_t count;
       iarc >> success >> count;
       if (count > 0) {
         *out = new T[count];
         for (size_t i = 0; i < count; i++) 
         iarc >> (*out)[i];
       }
       *size = count;
       if (!success) {
         iarc >> errormsgs;
         for (size_t i = 0; i < errormsgs.size(); i++) {
           logstream(LOG_ERROR) << errormsgs[i] << std::endl;
         }
       }
       return success;
     }

     /**
      * Parse a reply that has no content. 
      */
     inline bool parse_reply(std::string reply, std::string& errormsg) {
       iarchive iarc(reply.c_str(), reply.length());
       bool success;
       iarc >> success;
       if (!success) {
         iarc >> errormsg;
         logstream(LOG_ERROR) <<  errormsg << std::endl;
       } 
       return success;
     }

     // template<typename F, typename T>
     // bool parse_future_reply(std::vector<query_result>& futures, F fun, T& acc, std::string& errormsg) {
     //   bool success = true;
     //   for (size_t i = 0; i < futures.size(); i++) {
     //     T next;
     //     if (futures[i].get_status() == 0) {
     //       bool succ = parse_reply(futures[i].get_reply(), next, errormsf);
     //       if (succ) {
     //         acc = fun(acc, next);
     //       } else {
     //         return false;
     //       }
     //     } else {
     //       errormsg = "Server connection failed";
     //       logstream(LOG_ERROR) << errormsg;
     //       return false;
     //     }
     //   } 
     //   return true;
     // }
  }; // end of class
} // end of namespace
#include<graphlab/macros_undef.hpp>
#endif
