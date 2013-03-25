#ifndef GRAPHLAB_DATABASE_GRAPHDB_CLIENT_HPP
#define GRAPHLAB_DATABASE_GRAPHDB_CLIENT_HPP

#include<graphlab/database/graph_database.hpp>
#include<graphlab/database/graphdb_config.hpp>
#include<graphlab/database/graph_shard_manager.hpp>
#include<graphlab/database/graphdb_query_object.hpp>
#include<graphlab/database/query_message.hpp>
#include<map>
#include<set>

namespace graphlab {
  class graphdb_client: public graph_database {
   public:
     typedef graph_database::vertex_adj_descriptor vertex_adj_descriptor;
     typedef graph_database::vertex_insert_descriptor vertex_insert_descriptor;
     typedef graph_database::edge_insert_descriptor edge_insert_descriptor;
     typedef graph_database::mirror_insert_descriptor mirror_insert_descriptor;

     typedef std::map<graph_vid_t, std::set<graph_shard_id_t> > mirror_table_type;
     typedef graphdb_query_object::query_result query_result;

   public:
     /// Creates server with empty fields.
     graphdb_client(graphdb_config& config) : queryobj(config), shard_manager(config.get_nshards()) {} 
     virtual ~graphdb_client() {};

     // --------------------- Basic Queries ----------------------------
     uint64_t num_vertices() ; 
     uint64_t num_edges() ; 
     const std::vector<graph_field> get_vertex_fields(); 
     const std::vector<graph_field> get_edge_fields();

     // --------------------- Schema Modification API ----------------------
     /// Add a field to the vertex data schema
     int add_vertex_field(const graph_field& field);

     /// Add a field to the edge data schema
     int add_edge_field(const graph_field& field);

     // --------------------- Structure Modification API ----------------------
     int add_vertex(graph_vid_t vid, const graph_row& data);

     int add_edge(graph_vid_t source, graph_vid_t target, const graph_row& data);

     // --------------------- Batch Structure Modification API ----------------------
     bool add_vertices(const std::vector<vertex_insert_descriptor>& vertices,
                       std::vector<int>& errorcodes);

     bool add_edges(const std::vector<edge_insert_descriptor>& edges,
                    std::vector<int>& errorcodes);

     // --------------------- Single Query API -----------------------------------------
     // Read API
     int get_vertex(graph_vid_t vid, graph_row& out);
     int get_edge(graph_eid_t eid, graph_row& out);
     int get_vertex_adj(graph_vid_t vid, bool in_edges, vertex_adj_descriptor& out);

     // Write API
     int set_vertex(graph_vid_t vid, const graph_row& data);
     int set_edge(graph_eid_t eid, const graph_row& data);


     // --------------------- Batch Query API -----------------------------------------
     bool get_vertices (const std::vector<graph_vid_t>& vids,
                        std::vector<graph_row>& out, 
                        std::vector<int>& errorcodes);
     bool get_edges (const std::vector<graph_eid_t>& eids,
                     std::vector<graph_row>& out, 
                     std::vector<int>& errorcodes);
     bool set_edges(const std::vector<std::pair<graph_eid_t, graph_row> >& pairs,
                    std::vector<int>& errorcodes);
     bool set_vertices(const std::vector<std::pair<graph_vid_t, graph_row> >& pairs,
                       std::vector<int>& errorcodes);

   private:
     // ---------------------- Helper functions ---------------------------------------
     int add_vertex_mirror(graph_vid_t, const std::vector<graph_shard_id_t>& mirrors);

     bool add_vertex_mirrors(const std::vector<mirror_insert_descriptor>& vmirrors,
                             std::vector<int>& errorcodes);

     mirror_table_type mirror_table_from_edges (const std::vector<edge_insert_descriptor>& edges);

     template<typename Tin, typename Tout>
     void scatter_messages (QueryMessage::header query_header, 
                            const std::vector<Tin>& in_values,
                            boost::function<graph_shard_id_t (const Tin&)> get_shard,
                            std::vector<Tout>* out_values, std::vector<int>& errorcodes) {

       typedef std::map<graph_shard_id_t, std::vector<size_t> >::iterator map_iter_type;
       // group values by the shard id
       std::map<graph_shard_id_t, std::vector<size_t> > shard2valueid; 
       for (size_t i = 0; i < in_values.size(); i++) {
         graph_shard_id_t key = get_shard(in_values[i]);
         shard2valueid[key].push_back(i);
       }

       std::vector< std::pair<graph_shard_id_t, query_result> > replies;

       // for each shard send out the query
       for (map_iter_type it = shard2valueid.begin(); it != shard2valueid.end(); ++it) {
         graph_shard_id_t shardid = it->first;
         std::vector<size_t>& ids = it->second;

         // making query messages
         QueryMessage qm(query_header);
         std::vector<Tin> valuevec;
         for (size_t i = 0; i < ids.size(); ++i) {
           valuevec.push_back(in_values[ids[i]]);
         }
         qm << valuevec;
         query_result future = 
             (out_values == NULL) ? queryobj.update(shardid, qm.message(), qm.length())
                                  : queryobj.query(shardid, qm.message(), qm.length());
         replies.push_back(std::pair<graph_shard_id_t, query_result> (shardid, future));
       }


       errorcodes.resize(in_values.size(), 0);
       if (out_values != NULL)
         out_values->resize(in_values.size());

       // parse the reply 
       for (size_t i = 0; i < replies.size(); i++) {
         std::vector<int> errorcodes_i;
         std::vector<Tout> results_i;
         graph_shard_id_t shardid = replies[i].first;
         bool success = false;
         if (out_values == NULL) {
            success = queryobj.parse_batch_reply<char>(replies[i].second, NULL, errorcodes_i);
         } else {
            success = queryobj.parse_batch_reply<Tout>(replies[i].second, &results_i, errorcodes_i);
         }

         std::vector<size_t>& ids = shard2valueid[shardid];
         if (success) { errorcodes_i.resize(ids.size()); } // resize a vector of 0 error codes
         // fill the out vectors
         if (out_values != NULL) {
           for (size_t j = 0; j < ids.size(); j++) {
             (*out_values)[ids[j]] = results_i[j];
           }
         }

         for (size_t j = 0; j < ids.size(); j++) {
           errorcodes[ids[j]] = errorcodes_i[j];
         }
       }
    }
     
     // Compute shard id from different types 
     graph_shard_id_t eid2shard(const graph_eid_t& eid);
     graph_shard_id_t vid2shard(const graph_vid_t& vid);
     graph_shard_id_t edge2shard(const std::pair<graph_vid_t, graph_vid_t>& edge);
     graph_shard_id_t vin2shard(const vertex_insert_descriptor& des);
     graph_shard_id_t ein2shard(const edge_insert_descriptor& des);

     template<typename T>
     graph_shard_id_t eidpair2shard(const std::pair<graph_eid_t, T>& pair) {
       return split_eid(pair.first).first; 
     }

     template<typename T>
     graph_shard_id_t vidpair2shard(const std::pair<graph_vid_t, T>& pair) {
       return shard_manager.get_master(pair.first);
     }

   private:
     graphdb_query_object queryobj;
     graph_shard_manager shard_manager;
  };
}
#endif
