#ifndef GRAPHLAB_DATABASE_QUERY_OBJ_HPP
#define GRAPHLAB_DATABASE_QUERY_OBJ_HPP

#include<graphlab/database/graph_database_server.hpp>
namespace graphlab {
  class fake_query_obj {
    graph_database_server* server;
   public:
     fake_query_obj(graph_database_server* server) : server(server) { }

     std::string create_vfield_request() {
       graphlab::oarchive oarc;
       oarc << std::string("vertex_fields_meta");
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_efield_request() {
       graphlab::oarchive oarc;
       oarc << std::string("edge_fields_meta");
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_sharding_graph_request() {
       graphlab::oarchive oarc;
       oarc << std::string("sharding_graph");
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_vertex_row_request(graphlab::graph_vid_t vid) {
       graphlab::oarchive oarc;
       oarc << std::string("vertex_data_row") << vid;
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_shard_request(graph_shard_id_t shardid) {
       graphlab::oarchive oarc;
       oarc << std::string("shard_id") << shardid;
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_vertex_adj_request(graphlab::graph_vid_t vid,
                                           graphlab::graph_shard_id_t shardid,
                                           bool getin,
                                           bool getout,
                                           bool prefetch_data) {
       graphlab::oarchive oarc;
       oarc << std::string("vertex_adj") << vid << shardid << getin << getout << prefetch_data;
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_num_shards_request () {
       graphlab::oarchive oarc;
       oarc << std::string("num_shards"); 
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_num_verts_request () {
       graphlab::oarchive oarc;
       oarc << std::string("num_vertices"); 
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }

     std::string create_num_edges_request () {
       graphlab::oarchive oarc;
       oarc << std::string("num_edegs"); 
       std::string s(oarc.buf, oarc.off);
       free(oarc.buf);
       return s;
     }
  };
}

#endif
