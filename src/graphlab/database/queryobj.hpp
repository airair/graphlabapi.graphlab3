#ifndef GRAPHLAB_DATABASE_QUERY_OBJ_HPP
#define GRAPHLAB_DATABASE_QUERY_OBJ_HPP

#include<graphlab/database/graph_database_server.hpp>
namespace graphlab {
  class fake_query_obj {
    graph_database_server* server;
   public:
     fake_query_obj(graph_database_server* server) : server(server) { }


     // ------------- Query requests  ----------------
     std::string create_vfield_request() {
       oarchive oarc;
       oarc << std::string("vertex_fields_meta");
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_efield_request() {
       oarchive oarc;
       oarc << std::string("edge_fields_meta");
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_sharding_graph_request() {
       oarchive oarc;
       oarc << std::string("sharding_graph");
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_vertex_row_request(graph_vid_t vid) {
       oarchive oarc;
       oarc << std::string("vertex_data_row") << vid;
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_shard_request(graph_shard_id_t shardid) {
       oarchive oarc;
       oarc << std::string("shard") << shardid;
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_shard_content_adj_request (graph_shard_id_t from,
                                                   graph_shard_id_t to) {
       oarchive oarc;
       oarc << std::string("shard_content_adj") << from << to;
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_vertex_adj_request(graph_vid_t vid,
                                           graph_shard_id_t shardid,
                                           bool getin,
                                           bool getout,
                                           bool prefetch_data) {
       oarchive oarc;
       oarc << std::string("vertex_adj") << vid << shardid << getin << getout << prefetch_data;
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_num_shards_request () {
       oarchive oarc;
       oarc << std::string("num_shards"); 
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_num_verts_request () {
       oarchive oarc;
       oarc << std::string("num_vertices"); 
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_num_edges_request () {
       oarchive oarc;
       oarc << std::string("num_edges"); 
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     // ------------- Ingress requests  ----------------
     std::string create_add_vertex_request(graph_vid_t vid, 
                                           graph_shard_id_t shardid,
                                           graph_row* vdata = NULL) {
       oarchive oarc;
       oarc << std::string("add_vertex") << vid << shardid; 
       if (vdata != NULL) {
         oarc << true << *vdata;
       } else {
         oarc << false;
       }
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_add_edge_request(graph_vid_t source,
                                         graph_vid_t target,
                                         graph_shard_id_t shardid,
                                         graph_row* edata = NULL) {
       oarchive oarc;
       oarc << std::string("add_edge") << source << target << shardid; 
       if (edata != NULL) {
         oarc << true << *edata;
       } else {
         oarc << false;
       }

       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_add_vertex_mirror_request(graph_vid_t vid,
                                                  graph_shard_id_t master,
                                                  graph_shard_id_t mirror) {
       oarchive oarc;
       oarc << std::string("add_vertex_mirror") << vid << master << mirror; 
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }
  };
}
#endif
