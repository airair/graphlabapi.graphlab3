#ifndef GRAPHLAB_DATABASE_GRAPH_EDGE_INDEX
#define GRAPHLAB_DATABASE_GRAPH_EDGE_INDEX
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/unordered_map.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
  /** 
   * \ingroup group_graph_database_sharedmem
   * An index on edge id. 
   *
   * This class provides adjacency look up in one shard.
   */
  class graph_edge_index {
   public:
     /**
      * Fills in the query vid's incoming and outgoing edge index (in this shard)
      * into <code>in</code> and <code>out</code> vectors.
      * Skip the incoming (or outgoing) edge if getIn(or getOut) is false.
      */
     void get_edge_index (std::vector<size_t>& in,
                          std::vector<size_t>& out,
                          bool getIn, bool getOut,
                          graph_vid_t vid) {
       if (getIn && inEdges.find(vid) != inEdges.end()) {
           in = inEdges[vid];
       }
       if (getOut && outEdges.find(vid) != outEdges.end()) {
           out = outEdges[vid];
       }
     }

     /**
      * Update the index by adding an edge with (source, target, pos) in this shard. 
      */
    inline void add_edge(graph_vid_t source, graph_vid_t target, size_t pos) {
      outEdges[source].push_back(pos);
      inEdges[target].push_back(pos);
    }

    inline void save (oarchive& oarc) const {
       oarc << inEdges << outEdges;
     }
    inline void load (iarchive& iarc) {
       iarc >> inEdges >> outEdges;
     }

   private:
    // A vector where each element is a map from vid to a list of in edge ids on a shard.
    boost::unordered_map<graph_vid_t, std::vector<size_t> > inEdges;

    // A vector where each element is a map from vid to a list of out edge ids on a shard.
    boost::unordered_map<graph_vid_t, std::vector<size_t> > outEdges;
  };
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif

