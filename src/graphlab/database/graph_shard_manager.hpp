#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_MANAGER_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_MANAGER_HPP
#include <graphlab/database/basic_types.hpp>
#include <graphlab/logger/assertions.hpp>
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

    graph_shard_manager() {}

    /** Create n shards using given dependency 
     * \note ignore the method arg for now, only construct grid graph. 
     * \note assuming nshards is perfect square number
     */
    graph_shard_manager(size_t nshards, std::string method="grid") :
        nshards(nshards),  method(method) {
      make_grid_constraint();
      check();
    }

    /**
     * Returns the master shard of vertex(vid).
     */
    graph_shard_id_t get_master(graph_vid_t vid) const {
      return vidhash(vid) % num_shards();
    }

    /**
     * Returns the master shard of edge(source, target).
     */
    graph_shard_id_t get_master(graph_vid_t source,
                                graph_vid_t target) const {
      std::vector<graph_shard_id_t> candidates;
      get_joint_neighbors(get_master(source), get_master(target), candidates);
      ASSERT_GT(candidates.size(), 0);
      graph_shard_id_t shardid = candidates[edge_hash(std::pair<graph_vid_t, graph_vid_t>(source, target)) % candidates.size()];
      return shardid;
    }


    /**
     * Get the neighboring shards of shard. Fill in neighbors. 
     * Returns false on failure. 
     */
    bool get_neighbors (graph_shard_id_t shard, std::vector<graph_shard_id_t>& neighbors) const {
      ASSERT_LT(shard, nshards);
      neighbors = constraint_graph[shard];
      return true;
    }

    /**
     * Get the common neighbors of shardi and shardj. Fill in neighbors.
     * \note If shardi == shardj, the neighbors are defined as neighbors of shardi(shardj).
     * Returns false on failure. 
     */
    bool get_joint_neighbors (graph_shard_id_t shardi, graph_shard_id_t shardj, std::vector<graph_shard_id_t>& neighbors) const {
      ASSERT_EQ(neighbors.size(), 0);
      ASSERT_LT(shardi, nshards);
      ASSERT_LT(shardj, nshards);

      const std::vector<graph_shard_id_t>& ls1 = constraint_graph[shardi];
      const std::vector<graph_shard_id_t>& ls2 = constraint_graph[shardj];
      neighbors.clear();
      size_t i = 0;
      size_t j = 0;
      while (i < ls1.size() && j < ls2.size()) {
        if (ls1[i] == ls2[j]) {
          neighbors.push_back(ls1[i]);
          ++i; ++j;
        } else if (ls1[i] < ls2[j]) {
          ++i;
        } else {
          ++j;
        }
      }
      return true;
    }

    /**
     * Returns the number of shards.
     */
    size_t num_shards() const {
      return nshards;
    }

    void save (oarchive& oarc) const {
      oarc << nshards << method;
      for (size_t i = 0; i < nshards; i++) {
        oarc << constraint_graph[i]; 
      }
    }

    void load (iarchive& iarc) {
      iarc >> nshards >> method;
      constraint_graph.resize(nshards);
      for (size_t i = 0; i < nshards; i++) {
        iarc >> constraint_graph[i];
      }
    }
 
   private:
    void make_grid_constraint() {
      size_t ncols, nrows;
      ncols = nrows = (size_t)sqrt(nshards);

      for (size_t i = 0; i < nshards; i++) {
        std::vector<graph_shard_id_t> adjlist;
        // add self
        adjlist.push_back(i);

        // add the row of i
        size_t rowbegin = (i/ncols) * ncols;
        for (size_t j = rowbegin; j < rowbegin + ncols; ++j)
          if (i != j) adjlist.push_back(j); 

        // add the col of i
        for (size_t j = i % ncols; j < nshards; j+=ncols)
          if (i != j) adjlist.push_back(j); 

        std::sort(adjlist.begin(), adjlist.end());
        constraint_graph.push_back(adjlist);
      }
    }

    void check() {
      for (size_t i = 0; i < nshards; ++i) {
        for (size_t j = i+1; j < nshards; ++j) {
          std::vector<graph_shard_id_t> ls;
          get_joint_neighbors(i, j, ls);
          ASSERT_GT(ls.size(), 0);
        }
      }
    }
    // debug 
    // for (size_t i = 0; i < constraint_graph.size(); ++i) {
    //   std::vector<graph_shard_id_t> adjlist = constraint_graph[i];
    //   std::cout << i << ": [";
    //   for (size_t j = 0; j < adjlist.size(); j++)
    //     std::cout << adjlist[j] << " ";
    //   std::cout << "]" << std::endl;
    // }
  }; // end of sharding_constraint
}; // end of namespace graphlab
#endif
