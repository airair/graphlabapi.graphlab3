#include<graphlab/database/server/graph_database_server.hpp>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/client/graph_vertex_remote.hpp>
#include <graphlab/database/client/graph_edge_remote.hpp>
#include <graphlab/database/graph_database.hpp>

namespace graphlab {

  // ----------- Handler of basic statistics ------------------
  void graph_database_server::get_vertex_fields(iarchive& iarc, oarchive& oarc) {
    const std::vector<graph_field>& fields = database->get_vertex_fields();
    oarc << true << fields;
  }

  void graph_database_server::get_edge_fields(iarchive& iarc, oarchive& oarc) {
    const std::vector<graph_field>& fields = database->get_edge_fields();
    oarc << true << fields;
  }

  void graph_database_server::get_num_vertices(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shardid;
    iarc >> shardid;
    graph_shard* shard = database->get_shard(shardid);
    if (shard == NULL) {
      oarc << false << error_messages.shard_not_found(shardid);
    } else {
      oarc << true << shard->num_vertices();
    }
  }

  void graph_database_server::get_num_edges(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shardid;
    iarc >> shardid;
    graph_shard* shard = database->get_shard(shardid);
    if (shard == NULL) {
      oarc << false << error_messages.shard_not_found(shardid);
    } else {
      oarc << true << shard->num_edges();
    }
  }

  void graph_database_server::get_num_shards(iarchive& iarc, oarchive& oarc) {
    oarc << true << database->num_shards();
  }

  void graph_database_server::get_shard_list(iarchive& iarc, oarchive& oarc) {
    oarc << true << database->get_shard_list();
  }


  // -----------  GET Vertex Handlers ------------------
  void graph_database_server::get_vertex(iarchive& iarc, oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t shardid;
    iarc >> vid >> shardid;
    graph_vertex* v = database->get_vertex(vid, shardid);
    if (v == NULL) {
     oarc << false << error_messages.vertex_not_found(vid, shardid);
    } else {
      oarc << true;
      graph_vertex_remote::external_save(oarc, v);
      database->free_vertex(v);
    }
  }

  void graph_database_server::get_vertex_data_row(iarchive& iarc, oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t shardid;
    iarc >> vid >> shardid;
    graph_vertex* v = database->get_vertex(vid, shardid);
    if (v == NULL) {
      oarc << false << error_messages.vertex_not_found(vid, shardid);
    } else {
      const graph_row* row= v->immutable_data();
      oarc << true << *row;
      database->free_vertex(v);
    }
  }

  void graph_database_server::get_vertex_adj(iarchive& iarc, oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t shardid;
    bool get_in, get_out;
    iarc >> vid >> shardid >> get_in >> get_out;

    std::vector<graph_edge*> _inadj;
    std::vector<graph_edge*> _outadj;
    std::vector<graph_edge*>* out_inadj = get_in ? &(_inadj) : NULL;
    std::vector<graph_edge*>* out_outadj = get_out ? &(_outadj) : NULL;

    database->get_adj_list(vid, shardid, true, out_inadj, out_outadj);

    // Serialization protocol matches graph_edge_remote::vertex_adjacency_record::load
    oarc << true << vid << shardid << _inadj.size() << _outadj.size();
    if (get_in) {
      for (size_t i = 0; i < _inadj.size(); i++) {
         oarc << _inadj[i]->get_src() << _inadj[i]->get_id() << *(_inadj[i]->immutable_data());
      }
      database->free_edge_vector(_inadj);
    }
    if (get_out) {
      for (size_t i = 0; i < _outadj.size(); i++) {
        oarc << _outadj[i]->get_dest() << _outadj[i]->get_id() << *(_outadj[i]->immutable_data());
      }
      database->free_edge_vector(_outadj);
    }
  }

  // --------------- Get Edge Handlers --------------------
  void graph_database_server::get_edge(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shardid;
    graph_eid_t eid;
    iarc >> eid >> shardid;
    graph_edge* e = database->get_edge(eid, shardid);
    if (e == NULL) {
      oarc << false << error_messages.edge_not_found(eid, shardid);
    } else {
      oarc << true;
      graph_edge_remote::external_save(oarc, e);
      database->free_edge(e);
    }
  }

  void graph_database_server::get_edge_data_row(iarchive& iarc, oarchive& oarc) {
    graph_eid_t eid;
    graph_shard_id_t shardid;
    iarc >> eid >> shardid;
    graph_edge* e = database->get_edge(eid, shardid);
    if (e == NULL) {
      oarc << false << error_messages.edge_not_found(eid, shardid);
    } else {
      const graph_row* row= e->immutable_data();
      oarc << true << *row;
      database->free_edge(e);
    }
  }

  // ----------------- Get Shard Handlers ---------------------
  void graph_database_server::get_shard(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shardid;
    iarc >> shardid;
    graph_shard* shard = database->get_shard(shardid);
    if (shard == NULL) {
      oarc << false <<  error_messages.shard_not_found(shardid);
    } else {
      oarc << true << *shard;
    }
  }

  void graph_database_server::get_shard_contents_adj_to(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shard_to;
    std::vector<graph_vid_t> vids;
    iarc >> vids >> shard_to;
    graph_shard* shard = database->get_shard_contents_adj_to(vids, shard_to);
    if (shard == NULL) {
      oarc << false <<  error_messages.shard_not_found(shard_to);
    } else {
      oarc << true << *shard;
    }
  }


  // ----------- Modification Handlers ----------------
  void graph_database_server::add_field(iarchive& iarc,
                                        oarchive& oarc) {
    bool is_vertex, success;
    graph_field field_to_add;
    iarc >> is_vertex >> field_to_add;
    if (is_vertex) {
      success = get_database()->add_vertex_field(field_to_add);
    } else {
      success = get_database()->add_edge_field(field_to_add);
    }
    if (!success) {
      oarc << false << error_messages.field_duplicate(field_to_add.name);
    } else {
      oarc << true;
    }
  }

  void graph_database_server::remove_field(iarchive& iarc,
                                           oarchive& oarc) {
    bool is_vertex, success;
    size_t field_to_remove;
    iarc >> is_vertex >> field_to_remove;
    if (is_vertex) {
      success = get_database()->remove_vertex_field(field_to_remove);
    } else {
      success = get_database()->remove_edge_field(field_to_remove);
    }
    if (!success) {
      oarc << false << error_messages.field_not_found(field_to_remove);
    } else {
      oarc << true;
    }
  }

  void graph_database_server::set_vertex_field (iarchive& iarc,
                                                oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t shardid;
    size_t fieldpos;
    graph_value new_value;
    bool delta;
    iarc >> vid >> shardid >> fieldpos >> delta >> new_value;
    graph_shard* shard = database->get_shard(shardid);
    if (shard != NULL && shard->has_vertex(vid)) {
      bool success = database->set_field(shard->vertex_data_by_id(vid), fieldpos, new_value, delta);
      if (!success) {
        oarc << false << error_messages.fail_setting_value(fieldpos);
      } else {
        oarc << true;
      }
    } else {
      oarc << false << ((shard == NULL) ? error_messages.shard_not_found(shardid)
           : error_messages.vertex_not_found(vid, shardid));
    }
  }

  void graph_database_server::set_edge_field(iarchive& iarc,
                                             oarchive& oarc) {
    graph_eid_t eid;
    graph_shard_id_t shardid;
    graph_value new_value;
    size_t fieldpos;
    bool delta;
    iarc >> eid >> shardid >> fieldpos >> delta >> new_value;
    graph_shard* shard = database->get_shard(shardid);
    if (shard != NULL && eid < shard->num_edges()) {
      bool success = database->set_field(shard->edge_data(eid), fieldpos, new_value, delta);
      if (!success) {
        oarc << false << error_messages.fail_setting_value(fieldpos);
      } else {
        oarc << true;
      }
    } else {
      oarc << false << ((shard == NULL) ? error_messages.shard_not_found(shardid)
           : error_messages.edge_not_found(eid, shardid));
    }
  }

  void graph_database_server::set_vertex_row(iarchive& iarc,
                                             oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t shardid;
    size_t num_changes, fieldpos;
    iarc >> vid >> shardid >> num_changes;
    graph_shard* shard = database->get_shard(shardid);
    if (shard != NULL && shard->has_vertex(vid)) {
      graph_row* row = shard->vertex_data_by_id(vid);
      std::vector<std::string> errors;
      bool delta;
      graph_value  new_value;
      // set new values for all changed fields
      for (size_t i = 0; i < num_changes; i++) {
        iarc >> fieldpos >> delta >> new_value;
        bool success = database->set_field(row, fieldpos, new_value, delta);
        if (!success) {
          errors.push_back(error_messages.fail_setting_value(fieldpos));
        }
      }
      if (errors.size() > 0) {
        oarc << false << boost::algorithm::join(errors, "\n");
      } else {
        oarc << true;
      }
    } else {
      oarc << false << ((shard == NULL) ? error_messages.shard_not_found(shardid)
           : error_messages.vertex_not_found(vid, shardid));
    }
  }

  void graph_database_server::set_edge_row(iarchive& iarc, oarchive& oarc) {
    graph_eid_t eid;
    graph_shard_id_t shardid;
    size_t num_changes, fieldpos;
    iarc >> eid >> shardid >> num_changes;

    graph_shard* shard = database->get_shard(shardid);
    if (shard != NULL && eid < shard->num_edges()) {
      graph_row* row = shard->edge_data(eid);
      std::vector<std::string> errors;
      bool delta;
      graph_value  new_value;
      for (size_t i = 0; i < num_changes; i++) {
        iarc >> fieldpos >> delta >> new_value; 
        bool success = database->set_field(row, fieldpos, new_value, delta);
        if (!success) {
          errors.push_back(error_messages.fail_setting_value(fieldpos));
        }
      } 
      if (errors.size() > 0) {
        oarc << false << boost::algorithm::join(errors, "\n");
      } else {
        oarc << true;
      }
    } else {
      oarc << false << ((shard == NULL) ? error_messages.shard_not_found(shardid)
           : error_messages.edge_not_found(eid, shardid));
    }
  }

  // ------------------ Ingress Handlers -----------------
  void graph_database_server::add_vertex (iarchive& iarc,
                                          oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t master;
    bool hasdata, success;
    iarc >> vid >> master >> hasdata;
    if (hasdata) {
      graph_row* data = new graph_row;
      iarc >> *data;
      success = database->add_vertex(vid, master, data);
    } else {
      success = database->add_vertex(vid, master);
    }
    if (!success) {
      oarc << false << error_messages.fail_adding_vertex(vid);
    } else {
      oarc << true;
    }
  }

  void graph_database_server::add_edge (iarchive& iarc,
                                        oarchive& oarc) {
    graph_vid_t source, target;
    graph_shard_id_t master;
    bool hasdata, success;
    iarc >> source >> target >> master >> hasdata;
    if (hasdata) {
      graph_row* data = new graph_row;
      iarc >> *data;
      success = database->add_edge(source, target, master, data);
    } else {
      success = database->add_edge(source, target, master);
    }
    if (!success) {
      oarc << false << error_messages.fail_adding_edge(source, target);
    } else {
      oarc << true;
    }
  }

  void graph_database_server::add_vertex_mirror (iarchive& iarc,
                                                 oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t master, mirror;
    size_t size;
    iarc >> vid >> master >> size;
    bool success;
    for (size_t i = 0; i < size; i++) {
      iarc >> mirror;
      success = database->add_vertex_mirror(vid, master, mirror);
    }

    if (!success) {
      oarc << false << error_messages.fail_adding_mirror(vid, master, mirror);
    } else {
      oarc << true;
    }
  }



  // ----------------------- Batch Ingress Handler -----------------
  void graph_database_server::batch_add_vertex (iarchive& iarc,
                                                oarchive& oarc) {
    graph_shard_id_t master;
    size_t num_records;
    iarc >> master >> num_records;
    std::vector<std::string> errmsgs;

    for (size_t i = 0; i < num_records; i++) {
      bool success;
      graph_vid_t vid;
      bool hasdata;
      iarc >> vid >> hasdata;
      if (hasdata) {
        graph_row* data = new graph_row;
        iarc >> *data;
        success = database->add_vertex(vid, master, data);
      } else {
        success = database->add_vertex(vid, master);
      }
      if (!success) {
        errmsgs.push_back(error_messages.fail_adding_vertex(vid));
      }
    }

    if (errmsgs.size() > 0) {
      oarc << false << errmsgs;
    } else {
      oarc << true;
    }
  }

  void graph_database_server::batch_add_edge (iarchive& iarc,
                                              oarchive& oarc) {
    std::vector<std::string> errmsgs;
    graph_shard_id_t master;
    size_t num_records;
    iarc >> master >> num_records;

    for (size_t i = 0; i < num_records; ++i) {
      bool success;
      graph_vid_t source, target;
      bool hasdata;
      iarc >> source >> target >> hasdata;
      if (hasdata) {
        graph_row* data = new graph_row;
        iarc >> *data;
        success = database->add_edge(source, target, master, data);
      } else {
        success = database->add_edge(source, target, master);
      }
      if (!success) {
        errmsgs.push_back(error_messages.fail_adding_edge(source, target));
      }
    }
    if (errmsgs.size() > 0) {
      oarc << false << errmsgs;
    } else {
      oarc << true;
    }
  }

  void graph_database_server::batch_add_vertex_mirror (iarchive& iarc,
                                                       oarchive& oarc) {
    graph_shard_id_t master;
    size_t num_records;
    std::vector<std::string> errmsgs;
    iarc >> master >> num_records;
    for (size_t i = 0; i < num_records; i++) {
      bool success;
      graph_vid_t vid;
      size_t num_mirrors;
      graph_shard_id_t mirror;
      iarc >> vid >> num_mirrors;
      for (size_t j = 0; j <  num_mirrors; j++) {
        iarc >> mirror;
        success = database->add_vertex_mirror(vid, master, mirror);
      } 
      if (!success) {
        errmsgs.push_back(error_messages.fail_adding_mirror(vid, master, mirror));
      }
    }
    if (errmsgs.size() > 0) {
      oarc << false << errmsgs;
    } else {
      oarc << true;
    }
  }

  // ---------------------------- Batch Queries --------------------------------
  void graph_database_server::batch_get_vertices(iarchive& iarc, oarchive& oarc) {
    std::vector<graph_vid_t> vids;
    std::vector<graph_vertex*> vertices;
    std::vector<std::string> errormsgs;
    graph_shard_id_t shardid;

    iarc >> vids >> shardid;
    for (size_t i = 0; i < vids.size(); i++) {
      graph_vid_t vid = vids[i];
      graph_vertex* v = database->get_vertex(vid, shardid);
      if (v == NULL) {
        errormsgs.push_back(error_messages.vertex_not_found(vid, shardid));
      } else {
        vertices.push_back(v);
      }
    }

    bool success = errormsgs.size() == 0;
    oarc << success << vertices.size();
    for (size_t i = 0; i < vertices.size(); i++)  {
      graph_vertex_remote::external_save(oarc, vertices[i]);
      database->free_vertex(vertices[i]);
    }

    if (!success) {
      oarc << errormsgs;
    }
  }

  void graph_database_server::batch_get_vertex_adj_to_shard(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shard_from, shard_to;
    iarc >> shard_from >> shard_to;
    graph_shard* shard = database->get_shard(shard_from);
    std::vector<std::string> errormsgs;

    if (shard == NULL) {
       size_t count = 0;
       errormsgs.push_back(error_messages.shard_not_found(shard_from));
       oarc << false << count << errormsgs;
       return;
    } 

      // scan the mirrors see if it contains shard_to.
      std::vector<graph_vertex*> vertices;
      for (size_t i = 0; i < shard->num_vertices(); i++) {
        const boost::unordered_set<graph_shard_id_t>& mirrors = shard->mirrors(i);
        if (mirrors.find(shard_to) != mirrors.end()) {
          graph_vid_t vid = shard->vertex(i); 
          graph_vertex* v = database->get_vertex(vid, shard_from);
          if (v == NULL) {
            errormsgs.push_back(error_messages.vertex_not_found(vid, shard_from));
          } else {
            vertices.push_back(v);
          }
        }
      }

      bool success = errormsgs.size() == 0;
      oarc << success << vertices.size();
      for (size_t i = 0; i < vertices.size(); i++) {
        graph_vertex* v = vertices[i];
        graph_vertex_remote::external_save(oarc, v);
        database->free_vertex(v);
      }
      if (!success) 
        oarc << errormsgs;
  }
}
