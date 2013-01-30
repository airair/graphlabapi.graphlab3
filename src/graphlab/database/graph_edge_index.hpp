#ifndef GRAPHLAB_DATABASE_GRAPH_EDGE_INDEX
#define GRAPHLAB_DATABASE_GRAPH_EDGE_INDEX
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <boost/unordered_map.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
  class graph_edge_index {
    std::vector<boost::unordered_map<graph_vid_t, std::vector<size_t> > > inEdges;
    std::vector<boost::unordered_map<graph_vid_t, std::vector<size_t> > > outEdges;
   public:
     void get_edge_index (vector<size_t>& in,
                          vector<size_t>& out,
                          bool getIn,
                          bool getOut,
                          shard_id_t shard_id,
                          graph_vid_t vid) {
       if (getIn && inEdges[shard_id].find(vid) != NULL) {
           in = inEdges[shard_id][vid];
       }
       if (getOut && outEdges[shard_id].find(vid) != NULL) {
           out = outEdges[shard_id][vid];
       }
     }
  }
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif

