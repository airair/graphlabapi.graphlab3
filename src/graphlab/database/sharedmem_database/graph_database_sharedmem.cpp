#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
namespace graphlab {

  graph_database_sharedmem::graph_database_sharedmem(
      const std::vector<graph_field>& vertex_fields,
      const std::vector<graph_field>& edge_fields, 
      size_t numshards) : vertex_fields(vertex_fields), edge_fields(edge_fields) { 
        shardarr = new graph_shard[numshards];
        for (size_t i = 0; i < numshards; i++) {
          shardarr[i].shard_impl.shard_id = i;
          shard_list.push_back(i);
          shards[i] = &shardarr[i];
        }
      }

  graph_database_sharedmem::graph_database_sharedmem(
      const std::vector<graph_field>& vertex_fields,
      const std::vector<graph_field>& edge_fields, 
      const std::vector<graph_shard_id_t>& shard_list, size_t numshards)
      : vertex_fields(vertex_fields), edge_fields(edge_fields), shard_list(shard_list) { 

    shardarr = new graph_shard[shard_list.size()];
    for (size_t i = 0; i < shard_list.size(); i++) {
      shardarr[i].shard_impl.shard_id = shard_list[i];
      shards[shard_list[i]] = &shardarr[i];
    }
  }

  bool graph_database_sharedmem::add_vertex_field (graph_field& field) {
    if (find_vertex_field(field.name.c_str()) >= 0) {
      return false;
    } else {
      boost::function<void (graph_row& row)> fun(boost::bind(&graph_database_sharedmem::add_field_helper, this, _1, field));
      transform_vertices(fun);
      vertex_fields.push_back(field);
      return true;
    }
  }


  bool graph_database_sharedmem::add_edge_field (graph_field& field) {
    if (find_edge_field(field.name.c_str()) >= 0) {
      return false;
    } else {
      boost::function<void (graph_row& row)> fun(boost::bind(&graph_database_sharedmem::add_field_helper, this, _1, field));
      transform_edges(fun);
      edge_fields.push_back(field);
      return true;
    }
  }

  bool graph_database_sharedmem::remove_vertex_field (size_t fieldpos) {
    if (fieldpos >= vertex_fields.size()) {
      return false;
    } else {
      boost::function<void (graph_row& row)> fun(boost::bind(&graph_database_sharedmem::remove_field_helper, this, _1, fieldpos));
      transform_vertices(fun);
      vertex_fields.erase(vertex_fields.begin() + fieldpos);
      return true;
    }
  }

  bool graph_database_sharedmem::remove_edge_field (size_t fieldpos) {
    if (fieldpos >= edge_fields.size()) {
      return false;
    } else {
      boost::function<void (graph_row& row)> fun(boost::bind(&graph_database_sharedmem::remove_field_helper, this, _1, fieldpos));
      transform_edges(fun);
      edge_fields.erase(edge_fields.begin() + fieldpos);
      return true;
    }
  }


  bool graph_database_sharedmem::set_field(graph_row* row,
      size_t fieldpos, const graph_value& new_value, bool delta) {
    if (row != NULL && fieldpos < row->num_fields()) { 
      graph_value* value = row->get_field(fieldpos);
      return value->set_val(new_value, delta);
    } else {
      return false;
    }
  }

  bool graph_database_sharedmem::reset_field(bool is_vertex, size_t fieldpos,
                                             std::string& value) {
    boost::function<bool (graph_row& row)> fun(boost::bind(&graph_database_sharedmem::reset_field_helper, this, _1, fieldpos, value));
    if (is_vertex && fieldpos < vertex_fields.size()) {
      transform_vertices(fun);
    } else if (!is_vertex && fieldpos < edge_fields.size()) {
      transform_edges(fun);
    } else {
      return false;
    }
    return true;
  }

  graph_vertex* graph_database_sharedmem::get_vertex(graph_vid_t vid, graph_shard_id_t shardid) {
    if (shards[shardid] == NULL || !shards[shardid]->has_vertex(vid)) {
      return NULL;
    }
    return (new graph_vertex_sharedmem(vid, shardid, this));
  };


  graph_edge* graph_database_sharedmem::get_edge(graph_eid_t eid, graph_shard_id_t shardid) {
    if (shards[shardid] == NULL || eid >= shards[shardid]->num_edges()) {
      return NULL;
    } else {
      return (new graph_edge_sharedmem(eid, shardid, this));
    } 
  }

  void graph_database_sharedmem::get_adj_list(graph_vid_t vid, graph_shard_id_t shard_id,
                                              bool prefetch_data,
                                              std::vector<graph_edge*>* out_inadj,
                                              std::vector<graph_edge*>* out_outadj) {
    graph_shard* shard = get_shard(shard_id);
    ASSERT_TRUE(shard != NULL);
    std::vector<size_t> index_in;
    std::vector<size_t> index_out;
    bool getIn = !(out_inadj == NULL);
    bool getOut = !(out_outadj == NULL);
    shard->shard_impl.edge_index.get_edge_index(index_in, index_out, getIn, getOut, vid);

    if (getIn) {
      graph_edge_sharedmem* inadj = new graph_edge_sharedmem[index_in.size()];
      for(size_t i = 0; i < index_in.size(); i++) {
        std::pair<graph_vid_t, graph_vid_t> pair = shard->edge(index_in[i]);
        inadj[i].eid = index_in[i];
        inadj[i].master = shard_id;
        inadj[i].database = this;
        out_inadj->push_back(&inadj[i]);
      }
    }
    if (getOut) {
      graph_edge_sharedmem* outadj = new graph_edge_sharedmem[index_out.size()];
      for (size_t i = 0; i < index_out.size(); i++) {  
        std::pair<graph_vid_t, graph_vid_t> pair = shard->edge(index_out[i]);
        outadj[i].eid = index_out[i];
        outadj[i].master = shard_id;
        outadj[i].database = this;
        out_outadj->push_back(&outadj[i]);
      }
    }
  }

  graph_shard* graph_database_sharedmem::get_shard_contents_adj_to(graph_shard_id_t shard_id,
                                                                   graph_shard_id_t adjacent_to) {
    if (shards[shard_id] == NULL || shards[adjacent_to] == NULL) {
      return NULL;
    }

    const std::vector<graph_vid_t>& vids = shards[shard_id]->shard_impl.vertex;
    return get_shard_contents_adj_to(vids, adjacent_to);
  }

  graph_shard* graph_database_sharedmem::get_shard_contents_adj_to(const std::vector<graph_vid_t>& vids, 
                                                                   graph_shard_id_t adjacent_to) {

    graph_shard* ret = new graph_shard();
    graph_shard_impl& shard_impl = ret->shard_impl;
    shard_impl.shard_id = adjacent_to;

    boost::unordered_set<graph_eid_t> eids;

    // For each vertex in vids, if its master or mirrors conatins adjacent_to, then copy its adjacent edges from adjacent_to. 
    for (size_t i = 0; i < vids.size(); i++) {
      std::vector<size_t> index_in;
      std::vector<size_t> index_out;
      shards[adjacent_to]-> shard_impl.edge_index.get_edge_index(index_in, index_out, true, true, vids[i]);

      // copy incoming edges vids[i]
      for (size_t j = 0; j < index_in.size(); j++) {
        // avoid adding the same edge twice
        if (eids.find(index_in[j]) == eids.end()) { 
          std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to]->edge(index_in[j]);
          graph_row* data = shards[adjacent_to]->edge_data(index_in[j]);
          graph_row* data_copy = new graph_row();
          data->deepcopy(*data_copy);
          shard_impl.add_edge(e.first, e.second, data_copy);
          shard_impl.edgeid.push_back(index_in[j]);
          eids.insert(index_in[j]);
          delete data_copy;
        }
      }

      // copy outgoing edges of vids[i]
      for (size_t j = 0; j < index_out.size(); j++) {
        // avoid adding the same edge twice
        if (eids.find(index_out[j]) == eids.end()) {
          std::pair<graph_vid_t, graph_vid_t> e = shards[adjacent_to]->edge(index_out[j]);
          graph_row* data = shards[adjacent_to]->edge_data(index_out[j]);
          graph_row* data_copy = new graph_row();
          data->deepcopy(*data_copy);
          shard_impl.add_edge(e.first, e.second, data_copy);
          shard_impl.edgeid.push_back(index_out[j]);
          eids.insert(index_out[j]);
          delete data_copy;
        }
      }
    }
    return ret;
  }

  graph_shard* graph_database_sharedmem::get_shard_copy(graph_shard_id_t shard_id) {
    if (shards[shard_id] == NULL) {
      return NULL;
    }
    graph_shard* ret = new graph_shard;
    shards[shard_id]->shard_impl.deepcopy(ret->shard_impl);
    return ret;
  }

  // void graph_database_sharedmem::commit_shard(graph_shard* shard) {
    // graph_shard_id_t id = shard->id();
    // ASSERT_TRUE(shards[id] != NULL);

    // // commit vertex data
    // for (size_t i = 0; i < shard->num_vertices(); i++) {
    //   graph_row* row = shard->vertex_data(i);
    //   for (size_t j = 0; j < row->num_fields(); j++) {
    //     graph_value* val = row->get_field(j);
    //     if (val->get_modified()) {
    //       val->post_commit_state();
    //     }
    //   }
    // }
    // // commit edge data
    // 
    // // if the shard to commit is a derived shard, we need to overwrite 
    // // the corresponding edges in the original shard
    // bool derivedshard = shard->shard_impl.edgeid.size() > 0;
    // for (size_t i = 0; i < shard->num_edges(); i++) {
    //   graph_row* local = shard->edge_data(i);
    //   graph_row* origin = derivedshard ? shards[id]->edge_data(shard->shard_impl.edgeid[i]) 
    //       : shards[id]->edge_data(i);
    //   ASSERT_TRUE(origin != NULL);
    //   for (size_t j = 0; j < local->num_fields(); j++) {
    //     graph_value* val = local->get_field(j);
    //     if (val->get_modified()) {
    //       val->post_commit_state();
    //       origin->get_field(j)->free_data();
    //       val->deepcopy(*origin->get_field(j));
    //     }
    //   }
    // }
  // }

  bool graph_database_sharedmem::add_vertex(graph_vid_t vid, graph_shard_id_t master, graph_row* data) {
      if (shards[master] == NULL) {
        return false;
      }
      if (shards[master]->has_vertex(vid)) { // vertex has already been inserted 
        if (data) {
          graph_row* row =  shards[master]->vertex_data_by_id(vid);
          if (row->is_null()) { // existing vertex has no value, update with new value
            data->copy_transfer_owner(*row);
            delete data;
            return true;
          } else { // existing vertex has value, cannot overwrite, return false
            return false;
          }
        } else { // new insertion does not have value, do nothing and return true;
          return true;
        }
      } else {
        // create a new row of all null values.
        graph_row* row = (data==NULL) ? new graph_row(vertex_fields, true) : data;
        row->_is_vertex = true;

        // Insert into shard. This operation transfers the data ownership to the row in the shard
        // so that we can free the row at the end of the function.
        shards[master]->shard_impl.add_vertex(vid, row);
        return true;
      }
  }

  bool graph_database_sharedmem::add_edge(graph_vid_t source, graph_vid_t target, graph_shard_id_t shard_id, graph_row* data) {
      if (shards[shard_id] == NULL) {
        return false;
      } 
      // create a new row of all null values
      graph_row* row = (data==NULL) ? new graph_row(edge_fields, false) : data;
      // Insert into shard. This operation transfers the data ownership to the row in the shard
      // so that we can free the row at the end of the function.
      shards[shard_id]->shard_impl.add_edge(source, target, row);
      return true;
    }

    /**
     * Add shard_id to the vertex mirror list.
     * Assuming the vertex to be updated is stored in a local shard.
     */
    bool graph_database_sharedmem::add_vertex_mirror (graph_vid_t vid, graph_shard_id_t master, graph_shard_id_t mirror_shard) {
      if (shards[master] == NULL) {
        return false;
      }
      if (!shards[master]->has_vertex(vid)) {
        add_vertex(vid, master);
      }
      shards[master]->shard_impl.add_vertex_mirror(vid, mirror_shard);
      return true;
    }
};
