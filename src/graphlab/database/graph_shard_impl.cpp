#include <graphlab/database/graph_shard_impl.hpp>

namespace graphlab {
  size_t graph_shard_impl::add_vertex(graph_vid_t vid, const graph_row& row) {
    vertex.push_back(vid);
    vertex_mirrors.push_back(boost::unordered_set<graph_shard_id_t>());
    vertex_data.push_back(row);
    size_t pos = vertex.size()-1;
    // update vertex index
    vertex_index.add_vertex(vid, &vertex_data[pos], pos);
    return pos; 
  }

  void graph_shard_impl::add_vertex_mirror(graph_vid_t v, graph_shard_id_t mirror_id) {
    if (mirror_id == shard_id)
      return;
    size_t pos = vertex_index.get_index(v);
    vertex_mirrors[pos].insert(mirror_id);
  }

  size_t graph_shard_impl::add_edge(graph_vid_t source, graph_vid_t target, const graph_row& row) {
    edge.push_back(std::pair<graph_vid_t, graph_vid_t>(source, target));
    edge_data.push_back(row);
    size_t pos = edge.size()-1; 
    edge_index.add_edge(source,target, pos);
    return pos;
  }

  void graph_shard_impl::load(iarchive& iarc) {
    iarc >> shard_id;
    iarc >> vertex;
    iarc >> vertex_data;
    iarc >> edgeid >> edge;
    iarc >> edge_data;
    iarc >> vertex_index >> edge_index >> vertex_mirrors;
  }

  void graph_shard_impl::save(oarchive& oarc) const {
    oarc << shard_id;
    oarc << vertex;
    oarc << vertex_data;
    oarc << edgeid << edge;
    oarc << edge_data;
    oarc << vertex_index << edge_index << vertex_mirrors;
  }
}
