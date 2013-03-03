#ifndef GRAPHLAB_DATABASE_QUERY_MESSAGES_HPP
#define GRAPHLAB_DATABASE_QUERY_MESSAGES_HPP

#include<fault/query_object_client.hpp>
#include<graphlab/macros_def.hpp>

namespace graphlab {

  class QueryMessages {
   public:
    typedef libfault::query_object_client::query_result query_result;

   public:
     struct vertex_record {
       graph_vid_t vid;
       const graph_row* data;
       vertex_record(graph_vid_t vid, const graph_row* data) : vid(vid), data(data) { }
     };

     struct edge_record {
       graph_vid_t source;
       graph_vid_t target;
       const graph_row* data;
       edge_record(graph_vid_t source, graph_vid_t target, const graph_row* data = NULL) :
           source(source), target(target), data(data) { }
     };

     struct mirror_record {
       graph_vid_t vid;
       boost::unordered_set<graph_shard_id_t> mirrors;
       mirror_record() {};
     };

   public:
     // ------------- Query requests  ----------------
     char* vfield_request(int* msg_len) {
       oarchive oarc;
       oarc << std::string("vertex_fields_meta");
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* efield_request(int* msg_len) {
       oarchive oarc;
       oarc << std::string("edge_fields_meta");
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* sharding_graph_request(int* msg_len) {
       oarchive oarc;
       oarc << std::string("sharding_graph");
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* vertex_row_request(int* msg_len, graph_vid_t vid) {
       oarchive oarc;
       oarc << std::string("vertex_data_row") << vid;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* vertex_request(int* msg_len, graph_vid_t vid) {
       oarchive oarc;
       oarc << std::string("vertex") << vid; 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* shard_request(int* msg_len, graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("shard") << shardid;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* shard_content_adj_request (int* msg_len,
                                      graph_shard_id_t from,
                                      graph_shard_id_t to) {
       oarchive oarc;
       oarc << std::string("shard_content_adj") << from << to;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* vertex_adj_request(int* msg_len,
                              graph_vid_t vid,
                              graph_shard_id_t shardid,
                              bool getin,
                              bool getout) {
       oarchive oarc;
       oarc << std::string("vertex_adj") << vid << shardid << getin << getout;
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* num_shards_request (int* msg_len) {
       oarchive oarc;
       oarc << std::string("num_shards"); 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* num_verts_request (int* msg_len) {
       oarchive oarc;
       oarc << std::string("num_vertices"); 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* num_edges_request (int* msg_len) {
       oarchive oarc;
       oarc << std::string("num_edges"); 
       *msg_len=oarc.off;
       return oarc.buf;
     }

     // ------------- Ingress requests  ----------------
     char* add_vertex_request(int* msg_len,
                              graph_shard_id_t shardid,
                              vertex_record& vrecord
                             ) {
       oarchive oarc;
       oarc << std::string("add_vertex") << vrecord.vid << shardid; 
       if (vrecord.data!= NULL) {
         oarc << true << *(vrecord.data);
       } else {
         oarc << false;
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* batch_add_vertex_request(int* msg_len,
                                    graph_shard_id_t shardid,
                                    std::vector<vertex_record>& vrecords) {
       ASSERT_TRUE(vrecords.size() > 0);
       oarchive oarc;
       oarc << std::string("batch_add_vertex") << shardid << vrecords.size(); 
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

     char* add_edge_request(int* msg_len,
                            graph_shard_id_t shardid,
                            edge_record& erecord) {
       oarchive oarc;
       oarc << std::string("add_edge") << erecord.source << erecord.target << shardid; 
       if (erecord.data != NULL) {
         oarc << true << *(erecord.data);
       } else {
         oarc << false;
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* batch_add_edge_request(int* msg_len,
                                  graph_shard_id_t shardid,
                                  std::vector<edge_record>& erecords) {
       ASSERT_TRUE(erecords.size() > 0);
       oarchive oarc;
       oarc << std::string("batch_add_edge") << shardid << erecords.size();
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

     char* add_vertex_mirror_request(int* msg_len,
                                     graph_shard_id_t shardid,
                                     mirror_record& mrecord){
       oarchive oarc;
       oarc << std::string("add_vertex_mirror") << mrecord.vid << shardid;
       oarc << mrecord.mirrors.size();
       foreach(graph_shard_id_t mirror, mrecord.mirrors) {
         oarc << mirror;
       }
       *msg_len=oarc.off;
       return oarc.buf;
     }

     char* batch_add_vertex_mirror_request(int* msg_len,
                                           graph_shard_id_t shardid,
                                           boost::unordered_map<graph_vid_t, mirror_record>& mrecords) {
       ASSERT_TRUE(mrecords.size() > 0);
       oarchive oarc;
       oarc << std::string("batch_add_vertex_mirror") << shardid << mrecords.size();

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

     bool parse_reply(std::string reply, std::string& errormsg) {
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
