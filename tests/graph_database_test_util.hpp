#include <graphlab/database/server/graph_shard_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include <vector>
namespace graphlab {
  class graph_database_test_util {
   public:
     /**
      * Creates a shard server hosting a random graph with provided arguments.
      */
     static graph_shard_server* createShardServer(size_t nverts,
                                                  size_t nedges,
                                                  graph_shard_id_t shardid,
                                                  const std::vector<graph_field>& vertexfields,
                                                  const std::vector<graph_field>& edgefields) {

       graph_shard_server* server = new graph_shard_server(shardid, vertexfields, edgefields);

       graph_row empty_vdata(vertexfields, true);
       for (size_t i = 0; i < nverts; i++) {
         server->add_vertex(i, empty_vdata);
       }

       graph_row empty_edata(edgefields, false);
       boost::hash<size_t> vertexhash;
       // Creates a random graph
       for (size_t i = 0; i < nedges; i++) {
         size_t source = vertexhash(i) % nverts;
         size_t target = vertexhash(-i) % nverts;
         server->add_edge(source, target, empty_edata);
       }
       return server;
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
     static bool compare_row(const graph_row& lhs, const graph_row& rhs) {
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


     static size_t get_master(graph_vid_t vid, size_t nshards) {
       return  vid % nshards;
     }

     static size_t get_master(graph_vid_t src, graph_vid_t dest, size_t nshards) {
       boost::hash<std::pair<size_t, size_t> >  edgehash;
       std::pair<size_t, size_t> pair(src, dest);
       return edgehash(pair) % nshards;
     }
  };
} // end of namespace
