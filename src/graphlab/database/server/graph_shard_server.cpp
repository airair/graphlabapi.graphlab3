#include<graphlab/database/server/graph_shard_server.hpp>
#include<graphlab/database/errno.hpp>
#include<graphlab/logger/assertions.hpp>
#include<boost/functional.hpp>
#include<boost/bind.hpp>
namespace graphlab {

  // -------------------- Query API -----------------------
  // Read API
  int graph_shard_server::graph_shard_server::get_vertex(graph_vid_t vid, graph_row& out) {
    if (!shard.has_vertex(vid)) {
      return EINVID;
    }
    out = *shard.vertex_data_by_id(vid);
    return 0;
  }

  int graph_shard_server::get_edge(graph_eid_t eid, graph_row& out) {
    std::pair<graph_shard_id_t, graph_leid_t> pair = split_eid(eid);
    if (pair.first != shard.id() || pair.second >= shard.num_edges()) {
      return EINVID;
    }
    out = *shard.edge_data(pair.second);
    return 0;
  }

  int graph_shard_server::get_vertex_adj(graph_vid_t vid, bool is_in_edges, vertex_adj_descriptor& out) {
    // internal index of the adjacency edges
    std::vector<graph_leid_t> internal_ids;
    shard.vertex_adj_ids(internal_ids, vid, is_in_edges);
    if (is_in_edges) {
      for (size_t i = 0; i < internal_ids.size(); ++i) {
        out.neighbor_ids.push_back(shard.edge(internal_ids[i]).first);
      }
    } else {
      for (size_t i = 0; i < internal_ids.size(); ++i) {
        out.neighbor_ids.push_back(shard.edge(internal_ids[i]).second);
      }
    }
    // make the internal id into global eids
    for (size_t i = 0; i < internal_ids.size(); i++) {
      out.eids.push_back(make_eid(shard.id(), internal_ids[i])); 
    }
    return 0;
  }

  // Write API
  int graph_shard_server::set_vertex(const graph_vid_t vid, const graph_row& data) {
    return set_data_helper(shard.vertex_data_by_id(vid), data);
  }

  int graph_shard_server::set_edge(const graph_eid_t eid, const graph_row& data) {
    std::pair<graph_shard_id_t, graph_leid_t> pair = split_eid(eid);
    if (pair.first != shard.id()) {
      return EINVID;
    }
    return set_data_helper(shard.edge_data(pair.second), data);
  }

  // ------------------- Batch Query API -------------------- 
  bool graph_shard_server::get_vertices(const std::vector<graph_vid_t>& vids,
                                        std::vector<graph_row>& out,
                                        std::vector<int>& errorcodes) {
    out.resize(vids.size());
    bool success = true;
    out.resize(vids.size());
    for (size_t i = 0; i < vids.size(); ++i) {
      int err = get_vertex(vids[i], out[i]);
      errorcodes.push_back(err);
      success &= (err == 0);
    }
    return success;
  }

  bool graph_shard_server::get_edges(const std::vector<graph_eid_t>& eids,
                                     std::vector<graph_row>& out,
                                     std::vector<int>& errorcodes) {
    out.resize(eids.size());
    bool success = true;
    out.resize(eids.size());
    for (size_t i = 0; i < eids.size(); ++i) {
      int err = get_edge(eids[i], out[i]);
      errorcodes.push_back(err);
      success &= (err == 0);
    }
    return success;
  }

   // Write API
  bool graph_shard_server::set_vertices(const std::vector<std::pair<graph_vid_t, graph_row> >& pairs,
                                        std::vector<int>& errorcodes) {
    bool success = true;
    for (size_t i = 0; i < pairs.size(); ++i) {
      int err = set_vertex(pairs[i].first, pairs[i].second);
      errorcodes.push_back(err);
      success &= (err == 0);
    }
    return success;
  }
  
  bool graph_shard_server::set_edges(const std::vector<std::pair<graph_eid_t, graph_row> >& pairs,
                                     std::vector<int>& errorcodes) {
    bool success = true;
    for (size_t i = 0; i < pairs.size(); i++) {
      int err = set_edge(pairs[i].first, pairs[i].second);
      errorcodes.push_back(err);
      success &= (err == 0);
    }
    return success;
  }
  
  // -------- Data Schema API ---------------------
  int graph_shard_server::add_vertex_field(const graph_field& field) {
    if (find_vertex_field(field.name.c_str()) >= 0) {
      return EDUP;
    } else {
      boost::function<void (graph_row& row)> fun(boost::bind(&graph_shard_server::add_field_helper, this, _1, field));
      transform_vertices(fun);
      vertex_fields.push_back(field);
      return 0;
    }
  }

  int graph_shard_server::add_edge_field(const graph_field& field) {
    if (find_edge_field(field.name.c_str()) >= 0) {
      return EDUP;
    } else {
      boost::function<void (graph_row& row)> fun(boost::bind(&graph_shard_server::add_field_helper, this, _1, field));
      transform_edges(fun);
      edge_fields.push_back(field);
      return 0;
    }
  }
     

  // -------- Modification API --------------
  int graph_shard_server::add_vertex(graph_vid_t vid, const graph_row& data) {
    int errorcode = 0;
    if (shard.has_vertex(vid)) { // vertex has already been inserted 
        graph_row* row =  shard.vertex_data_by_id(vid);
        if (row->is_null()) { // existing vertex has no value, update with new value
          *row = data;
        } else { // existing vertex has value, cannot overwrite, return false
          errorcode = EDUP;
        }
    } else {
      if (data.is_vertex()) {
        shard.add_vertex(vid, data);
      } else {
        errorcode = EINVTYPE;
      }
    }
    logstream(LOG_WARNING) << glstrerr(errorcode) 
                           << ": (" << vid << ":" << data << ") " << std::endl;
    return errorcode;
  }

  int graph_shard_server::add_edge(graph_vid_t source, graph_vid_t target, const graph_row& data) {
    if (data.is_edge()) {
      shard.add_edge(source, target, data);
      return 0;
    } else {
      logstream(LOG_WARNING) << glstrerr(EINVTYPE) 
                             << ": (" << source << "," << target 
                             << ": " << data << ") " << std::endl;
      return EINVTYPE;
    }
  }

  bool graph_shard_server::add_vertices(const std::vector<vertex_insert_descriptor>& vertices,
                                        std::vector<int>& errorcodes) {
    bool success = true;
    for (size_t i = 0; i < vertices.size(); ++i) {
      int err = add_vertex(vertices[i].vid, vertices[i].data);
      errorcodes.push_back(err);
      success &= (err== 0);
    }
    return success;
  }

  bool graph_shard_server::add_edges(const std::vector<edge_insert_descriptor>& edges,
                                        std::vector<int>& errorcodes) {
    bool success = true;
    for (size_t i = 0; i < edges.size(); ++i) {
      int err = add_edge(edges[i].src, edges[i].dest, edges[i].data);
      errorcodes.push_back(err);
      success &= (err== 0);
    }
    return success;
  }

  bool graph_shard_server::add_vertex_mirrors(const std::vector<mirror_insert_descriptor>& vid_mirror_pairs, std::vector<int>& errorcodes) {
    bool success = true;
    for (size_t i = 0; i < vid_mirror_pairs.size(); i++) {
      int err = add_vertex_mirror(vid_mirror_pairs[i].first, vid_mirror_pairs[i].second);
      errorcodes.push_back(err);
      success &= (err == 0);
    }
    return success;
  }

  /**
   * Add shard_id to the vertex mirror list. Assuming the vertex to be updated is stored in a local shard.
   */
  int graph_shard_server::add_vertex_mirror(graph_vid_t vid, const std::vector<graph_shard_id_t>& mirrors) {
    int errorcode = 0;
    if (!shard.has_vertex(vid)) { 
      graph_row empty_row;
      shard.add_vertex(vid, empty_row);
    }
    for (size_t i = 0; i < mirrors.size(); i++) {
      shard.add_vertex_mirror(vid, mirrors[i]);
    }
    return errorcode;
  }

  // ---------- Helper functions -------------
  int graph_shard_server::set_data_helper(graph_row* old_data, const graph_row& data) {
    if (old_data == NULL || old_data->num_fields() != data.num_fields())
      return EINVID;
    for (size_t i = 0; i < data.num_fields(); ++i) {
      if (old_data->get_field(i)->type() != data.get_field(i)->type()) {
        return EINVTYPE;
      }
    }
    for (size_t i = 0; i < data.num_fields(); i++) {
      *old_data->get_field(i) = *data.get_field(i);
    }
    return 0;
  }
} // end of namespace
