#include <graphlab/database/graph_shard_impl.hpp>

namespace graphlab {
  size_t graph_shard_impl::add_vertex(graph_vid_t vid, graph_row* row) {
    ASSERT_TRUE(row->_own_data);
    size_t pos = num_vertices;
    vertex.push_back(vid);
    vertex_mirrors.push_back(boost::unordered_set<graph_shard_id_t>());

    // resize array, double the capacity.
    if (pos == _vdata_capacity) {
      _vdata_capacity *= 2;
      graph_row* vertex_data_tmp = new graph_row[_vdata_capacity];
      for (size_t i = 0; i < num_vertices; i++) {
        vertex_data[i].copy_transfer_owner(vertex_data_tmp[i]);
      }
      delete[] vertex_data;
      vertex_data = vertex_data_tmp;
    }

    row->copy_transfer_owner(vertex_data[pos]);

    num_vertices++;

    // update vertex index
    vertex_index.add_vertex(vid, &vertex_data[pos], pos);

    delete row;
    return pos;
  }

  void graph_shard_impl::add_vertex_mirror(graph_vid_t v, graph_shard_id_t mirror_id) {
    if (mirror_id == shard_id)
      return;
    size_t pos = vertex_index.get_index(v);
    vertex_mirrors[pos].insert(mirror_id);
  }

  size_t graph_shard_impl::add_edge(graph_vid_t source, graph_vid_t target, graph_row* row) {
    size_t pos = num_edges;
    edge.push_back(std::pair<graph_vid_t, graph_vid_t>(source, target));

    // Resize array, double the size
    if (pos == _edata_capacity) {
      _edata_capacity *= 2;
      graph_row* edge_data_tmp = new graph_row[_edata_capacity];
      for (size_t i = 0; i < num_edges; i++) {
        edge_data[i].copy_transfer_owner(edge_data_tmp[i]);
      }
      delete[] edge_data;
      edge_data = edge_data_tmp;
    }

    row->copy_transfer_owner(edge_data[pos]);
    edge_index.add_edge(source,target, pos);
    num_edges++;
    return pos;
  }

  void graph_shard_impl::deepcopy(graph_shard_impl& out) {
    out.shard_id = shard_id;
    out.num_vertices = num_vertices;
    out.num_edges = num_edges;
    out.vertex = vertex;
    out.edge = edge;
    out.edgeid = edgeid;
    out.vertex_index = vertex_index;
    out.edge_index = edge_index;
    out.vertex_mirrors = vertex_mirrors;

    // Resize the vertex data array.
        if (out._vdata_capacity <= num_vertices) {
          delete[] out.vertex_data;
          out._vdata_capacity = num_vertices;
          out.vertex_data = new graph_row[_vdata_capacity];
        }

    // Resize the edge data array.
    if (out._edata_capacity <= num_edges) {
      delete[] out.edge_data;
      out._edata_capacity = num_edges;
      out.edge_data = new graph_row[_edata_capacity];
    }

    // make a deep copy of all vertexdata.
    for (size_t i = 0; i < num_vertices; i++) {
      vertex_data[i].deepcopy(out.vertex_data[i]);
    }

    // make a deep copy of all edge data.
    for (size_t i = 0; i < num_edges; i++) {
      edge_data[i].deepcopy(out.edge_data[i]);
    }
  }

  void graph_shard_impl::load(iarchive& iarc) {
    iarc >> shard_id
        >> num_vertices
        >> num_edges;

    iarc >> vertex;
    if (vertex_data != NULL) {
      delete[] vertex_data;
    }
    if (edge_data != NULL) {
      delete[] edge_data;
    }
    vertex_data = new graph_row[num_vertices];
    for (size_t i = 0; i < num_vertices; ++i)
      iarc >> vertex_data[i];

    iarc >> edgeid >> edge;
    edge_data = new graph_row[num_edges];
    for (size_t i = 0; i < num_edges; ++i)
      iarc >> edge_data[i];

    iarc >> vertex_index >> edge_index >> vertex_mirrors;
  }

  void graph_shard_impl::save(oarchive& oarc) const {
    oarc << shard_id
         << num_vertices
         << num_edges;

    oarc << vertex;
    for (size_t i = 0; i < num_vertices; ++i)
      oarc << vertex_data[i];

    oarc << edgeid << edge;
    for (size_t i = 0; i < num_edges; ++i)
      oarc << edge_data[i];

    oarc << vertex_index << edge_index << vertex_mirrors;
  }

}
