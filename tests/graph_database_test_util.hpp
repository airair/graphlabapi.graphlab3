#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/logger/assertions.hpp>
#include <vector>
namespace graphlab {

  class graph_database_test_util {

   public:
     /**
      * Creates a random graph in the database with provided arguments.
      */
     static graph_database* createDatabase(size_t nverts,
                                           size_t nedges,
                                           size_t nshards,
                                           const std::vector<graph_field>& vertexfields,
                                           const std::vector<graph_field>& edgefields) {
       graph_database_sharedmem* db = 
           new graph_database_sharedmem (vertexfields, edgefields, nshards);

       boost::hash<size_t> hash; 

       for (size_t i = 0;i < nverts; i++) {
         db->add_vertex(i);
       }

       for (size_t i = 0; i < nedges; i++) {
         size_t source = hash(i) % nverts;
         size_t target = hash(-i) % nverts;
         db->add_edge(source, target);
       }
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
