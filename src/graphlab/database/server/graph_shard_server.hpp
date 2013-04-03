#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_SERVER_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_SERVER_HPP
#include <graphlab/database/graph_database.hpp>
namespace graphlab {
  class graph_shard_server : public graph_database {
   public:
     typedef graph_database::vertex_adj_descriptor vertex_adj_descriptor;
     typedef graph_database::vertex_insert_descriptor vertex_insert_descriptor;
     typedef graph_database::edge_insert_descriptor edge_insert_descriptor;
     typedef graph_database::mirror_insert_descriptor mirror_insert_descriptor;

   public:
     /// Creates server with empty fields.
     graph_shard_server(graph_shard_id_t shardid) : shard(shardid) { }

     /// Creates a server with fields and shard id.
     graph_shard_server(graph_shard_id_t shardid,
                        const std::vector<graph_field>& vertex_fields,
                        const std::vector<graph_field>& edge_fields) : 
         shard(shardid), vertex_fields(vertex_fields), edge_fields(edge_fields) { }


      ~graph_shard_server() {};

      void clear();
  // --------------------- Basic Queries ----------------------------
  uint64_t num_vertices() { return shard.num_vertices(); }
  uint64_t num_edges() { return shard.num_edges(); }
  const std::vector<graph_field> get_vertex_fields() { return vertex_fields; }
  const std::vector<graph_field> get_edge_fields() { return edge_fields; }

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

   bool set_vertices (const std::vector< std::pair<graph_vid_t, graph_row> >& pairs,
                      std::vector<int>& errorcodes);
   bool set_edges (const std::vector< std::pair<graph_eid_t, graph_row> >& pairs,
                   std::vector<int>& errorcodes);

  // --------------------- Internal functions --------------------------------
   graph_shard& get_shard() { return shard; }

   int add_vertex_mirror(graph_vid_t vid, const std::vector<graph_shard_id_t>& mirrors);

   bool add_vertex_mirrors(const std::vector<mirror_insert_descriptor>& vid_mirror_pairs, std::vector<int>& errorcodes);
 
   private:
     // --------------------- Helper functions -----------------------------------
     // Batch transform vertices 
     template<typename TransformFun> 
         void transform_vertices(TransformFun fun) {
           for (size_t i =0; i < shard.num_vertices(); ++i) {
             fun(*(shard.vertex_data(i)));
           }
         };

     // Batch transform edges 
     template<typename TransformFun> 
         void transform_edges(TransformFun fun) {
           for (size_t i = 0; i < shard.num_edges(); ++i) {
             fun(*(shard.edge_data(i)));
           }
         };

    inline void add_field_helper(graph_row& row, graph_field& field) { 
      row.add_field(field); 
    }

    inline bool reset_field_helper(graph_row& row, size_t fieldpos, std::string& value_str) {
        graph_value* val = row.get_field(fieldpos);
        if (val != NULL) {
          return val->set_val(value_str); 
        } else {
          return false;
        }
    }

    int set_data_helper(graph_row* old_data, const graph_row& data);

   private:
     graph_shard shard;
     std::vector<graph_field> vertex_fields;
     std::vector<graph_field> edge_fields;
  };
}// end of name space
#endif
