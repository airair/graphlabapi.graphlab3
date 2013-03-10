#include<graphlab/database/graph_shard_manager.hpp>
namespace graphlab {

  graph_shard_manager::graph_shard_manager(size_t nshards, std::string method)
      : nshards(nshards),  method(method) {
        make_grid_constraint();
        check();
      }

  graph_shard_id_t graph_shard_manager::get_master(graph_vid_t source,
                                                   graph_vid_t target) const {
    std::vector<graph_shard_id_t> candidates;
    get_joint_neighbors(get_master(source), get_master(target), candidates);
    ASSERT_GT(candidates.size(), 0);
    return candidates[edge_hash(std::pair<graph_vid_t, graph_vid_t>(source, target)) % candidates.size()];
  }

  bool graph_shard_manager::get_joint_neighbors (graph_shard_id_t shardi, graph_shard_id_t shardj, std::vector<graph_shard_id_t>& neighbors) const {
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

  void graph_shard_manager::make_grid_constraint() {
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

  void graph_shard_manager::check() {
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

}
