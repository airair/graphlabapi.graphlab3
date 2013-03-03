#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_sharding_constraint.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/logger/assertions.hpp>
#include <vector>
namespace graphlab {

  class graph_database_test_util {

   public:
     /**
      * Creates a database hosting a random graph with provided arguments.
      */
     static graph_database* createDatabase(size_t nverts,
                                           size_t nedges,
                                           size_t nshards,
                                           const std::vector<graph_field>& vertexfields,
                                           const std::vector<graph_field>& edgefields) {
       graph_database_sharedmem* db = 
           new graph_database_sharedmem (vertexfields, edgefields, nshards);

      const sharding_constraint& constraint_graph = db->get_sharding_constraint();

      for (size_t i = 0; i < nverts; i++) {
        db->add_vertex(i, constraint_graph.get_master(i));
      }

      boost::hash<size_t> hash; 

      // Creates a random graph
      for (size_t i = 0; i < nedges; i++) {
        size_t source = hash(i) % nverts;
        size_t target = hash(-i) % nverts;

        graphlab::graph_shard_id_t master = constraint_graph.get_master(source, target);
        db->add_edge(source, target, master);
        db->add_vertex_mirror(source, constraint_graph.get_master(source), master);
        db->add_vertex_mirror(target, constraint_graph.get_master(target), master);
      }
       return db;
     }

     /**
      * Creates a database hosting an a single shard of an empty graph with provided arguments.
      */
     static graph_database* createDatabase(graphlab::graph_shard_id_t shardid,
                                           const std::vector<graph_field>& vertexfields,
                                           const std::vector<graph_field>& edgefields,
                                           size_t nshards) {
       std::vector<graphlab::graph_shard_id_t> hostedshards;
       hostedshards.push_back(shardid);
       graph_database_sharedmem* db = 
           new graph_database_sharedmem (vertexfields, edgefields, hostedshards, nshards);
       return db;
     }


     /**
      * Return whether two graph values have the same content
      */
     static bool compare_value (const graph_value& lhs, const graph_value& rhs) {
       if ((lhs._type != rhs._type)
           || (lhs._null_value != rhs._null_value)
           || (lhs._len != rhs._len)) {
         return false;
       }
       bool eq = false;
       switch (lhs._type) {
        case INT_TYPE:
          eq = (lhs._data.int_value == rhs._data.int_value); break;
        case DOUBLE_TYPE:
          eq = (lhs._data.double_value == rhs._data.double_value); break;
        case VID_TYPE:
          eq = (lhs._data.vid_value == rhs._data.vid_value); break;
        case BLOB_TYPE:
        case STRING_TYPE:
          eq = ((memcmp(lhs._data.bytes, rhs._data.bytes, lhs._len)) == 0); break;
        default:
          eq = false;
       }
       return eq;
     }

     /**
      * Return whether two graph rows have the same content.
      */
     static bool compare_row(graph_row& lhs, graph_row& rhs) {
       if ((lhs.is_vertex() != rhs.is_vertex()) || (lhs.num_fields() != rhs.num_fields()))
         return false;

       bool eq = true;
       for (size_t j = 0; eq && j < lhs.num_fields(); j++) {
         eq = compare_value(*(lhs.get_field(j)), *(rhs.get_field(j)));
       }
       return eq;
     }

     /**
      * Return whether two graph shards have the same content.
      */
     static bool compare_shard(graph_shard& lhs, graph_shard& rhs) {
       bool eq = ((lhs.id() == rhs.id() && (lhs.num_vertices() == rhs.num_vertices())
                   && (lhs.num_edges() == rhs.num_edges())));
       for (size_t i = 0; i < lhs.num_vertices(); i++) {
           eq &= (lhs.vertex(i) == rhs.vertex(i));
           eq &= (compare_row(*lhs.vertex_data(i), *rhs.vertex_data(i)));
         }
       for (size_t i = 0; i < lhs.num_edges(); i++) {

           eq &= ((lhs.edge(i).first == rhs.edge(i).first) && (lhs.edge(i).second == rhs.edge(i).second));
           eq &= (compare_row(*lhs.edge_data(i), *rhs.edge_data(i)));
         }
       return eq;
     }


     /**
      * Return whether two graph field are the same. 
      */
     static bool compare_graph_field(const graph_field& lhs, const graph_field& rhs) {
       if ((lhs.name == rhs.name) && (lhs.is_indexed == rhs.is_indexed) 
           && (lhs.type == rhs.type) && (lhs.max_data_length == rhs.max_data_length)) {
         return true;
       } else {
        return false;
       }
     }
  };
}
