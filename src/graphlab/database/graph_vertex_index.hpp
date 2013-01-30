#ifndef GRAPHLAB_DATABASE_GRAPH_VERTEX_INDEX
#define GRAPHLAB_DATABASE_GRAPH_VERTEX_INDEX
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <boost/unordered_map.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
  /** 
   * \ingroup group_graph_database
   * An index on vertex id. 
   *
   * Provide lookup for the vertex locations (master, and mirrors...)
   */
  class graph_vertex_index {
   private:
     boost::unordered_map<graph_vid_t, graph_shard_id_t> master_map;  // map from vid -> shardid
     boost::unordered_map<graph_vid_t, std::vector<size_t> > location_map;  // map from vid -> locations on each shard
   public:
     bool has_vertex(graph_vid_t vid) {
       return !(master_map.find(vid) == master_map.end());
     };

     graph_shard_id_t get_master(graph_vid_t vid) {
       return master_map[vid];
     }

     std::vector<graph_shard_id_t> get_mirrors(graph_vid_t vid) {
       std::vector<graph_shard_id_t> mirrors;
       std::vector<size_t>& locations = location_map[vid];
       for (size_t i = 0; i < locations.size(); ++i) {
         if (locations[i] != (size_t)-1)
           mirrors.push_back(i);
       }
       return mirrors;
     }

     size_t get_index_in_shard (graph_vid_t vid, graph_shard_id_t shard) {
       return location_map[vid][shard];
     } 
  };
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif

