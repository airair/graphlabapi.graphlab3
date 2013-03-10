#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_MANAGER_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_MANAGER_HPP
#include <graphlab/database/basic_types.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>

#include <boost/functional/hash.hpp>
#include <algorithm>
#include <vector>

namespace graphlab {

  /**
   * \ingroup group_graph_database
   * A global object (shared among servers) which provides
   * information about shard dependencies. 
   * This also defines a deterministic mappings 
   * from vertex to shard, and from edge to shard
   */
  class graph_shard_manager {

    // number of shards in the distributed environment
    size_t nshards;

    // methods to create shard dependency: grid/pds 
    std::string method;

    // adjacency list encoding dependency among shards
    std::vector<std::vector<graph_shard_id_t> > constraint_graph;

    // Hash function for vertex id.
    boost::hash<graph_vid_t> vidhash;

    // Hash function for edge id.
    boost::hash<std::pair<graph_vid_t, graph_vid_t> > edge_hash;

   public:

    inline graph_shard_manager() : nshards(0) {}

    /** Create n shards using given dependency 
     * \note ignore the method arg for now, only construct grid graph. 
     * \note assuming nshards is perfect square number
     */
    graph_shard_manager(size_t nshards, std::string method="grid");

    /**
     * Returns the number of shards.
     */
    inline size_t num_shards() const {
      return nshards;
    }

    /**
     * Returns the master shard of vertex(vid).
     */
    inline graph_shard_id_t get_master(graph_vid_t vid) const {
      return vidhash(vid) % num_shards();
    }


    /**
     * Returns the master shard of edge(source, target).
     */
    graph_shard_id_t get_master(graph_vid_t source, graph_vid_t target) const; 


    /**
     * Get the neighboring shards of shard. Fill in neighbors. 
     * Returns false on failure. 
     */
    inline bool get_neighbors (graph_shard_id_t shard, std::vector<graph_shard_id_t>& neighbors) const {
      ASSERT_LT(shard, nshards);
      neighbors = constraint_graph[shard];
      return true;
    }

    /**
     * Get the common neighbors of shardi and shardj. Fill in neighbors.
     * \note If shardi == shardj, the neighbors are defined as neighbors of shardi(shardj).
     * Returns false on failure. 
     */
    bool get_joint_neighbors (graph_shard_id_t shardi, graph_shard_id_t shardj, std::vector<graph_shard_id_t>& neighbors) const;


    
    inline void save (oarchive& oarc) const {
      oarc << nshards << method;
      for (size_t i = 0; i < nshards; i++) {
        oarc << constraint_graph[i]; 
      }
    }

    inline void load (iarchive& iarc) {
      iarc >> nshards >> method;
      constraint_graph.resize(nshards);
      for (size_t i = 0; i < nshards; i++) {
        iarc >> constraint_graph[i];
      }
    }
 
   private:
    void make_grid_constraint();
    void check();
  }; // end of sharding_constraint
}; // end of namespace graphlab
#endif
