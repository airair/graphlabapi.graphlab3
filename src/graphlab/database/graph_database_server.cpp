#include<graphlab/database/graph_database_server.hpp>

namespace graphlab {

  void graph_database_server::get_vertex_fields(iarchive& iarc, oarchive& oarc) {
    const std::vector<graph_field>& fields = database->get_vertex_fields();
    oarc << true << fields;
  }

  void graph_database_server::get_edge_fields(iarchive& iarc, oarchive& oarc) {
    const std::vector<graph_field>& fields = database->get_edge_fields();
    oarc << true << fields;
  }

  void graph_database_server::get_sharding_constraint(iarchive& iarc, oarchive& oarc) {
    oarc << true << database->get_sharding_constraint();
  }

  void graph_database_server::get_num_vertices(iarchive& iarc, oarchive& oarc) {
    oarc << true << database->num_vertices();
  }

  void graph_database_server::get_num_edges(iarchive& iarc, oarchive& oarc) {
    oarc << true << database->num_edges();
  }

  void graph_database_server::get_num_shards(iarchive& iarc, oarchive& oarc) {
    oarc << true << database->num_shards();
  }

  void graph_database_server::get_vertex(iarchive& iarc, oarchive& oarc) {
    graph_vid_t vid;
    iarc >> vid;
    graph_vertex* v = database->get_vertex(vid);
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ") + boost::lexical_cast<std::string>(vid) + std::string(". Vid does not exist."));
      oarc << false << msg;
    } else {
      graph_shard_id_t master = v->master_shard();
      const std::vector<graph_shard_id_t>&  shards = v->get_shard_list();
      std::vector<graph_shard_id_t> mirrors;
      for (size_t i = 0; i < shards.size(); i++) {
        if (shards[i] != v->master_shard()) {
          mirrors.push_back(shards[i]);
        }
      } 
      oarc << true << vid << master << mirrors << true << *(v->data());
      database->free_vertex(v);
    }
  }

  void graph_database_server::get_edge(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shardid;
    graph_eid_t eid;
    iarc >> eid >> shardid;
    graph_edge* e = database->get_edge(eid, shardid);
    if (e == NULL) {
      std::string msg = (std::string("Fail to get edge id = ") + boost::lexical_cast<std::string>(eid) + " on shard id = " + boost::lexical_cast<std::string>(shardid) + std::string(". Edge does not exist."));
      oarc << false << msg;
    } else {
      graph_shard_id_t master = e->master_shard();
      oarc << true << e->get_src() << e->get_dest() << e->get_id() << master 
           << true << *(e->data());
      database->free_edge(e);
    }
  }


  void graph_database_server::get_vertex_data_row(iarchive& iarc, oarchive& oarc) {
    graph_vid_t vid;
    iarc >> vid;
    graph_vertex* v = database->get_vertex(vid);
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ") + boost::lexical_cast<std::string>(vid) + std::string(". Vid does not exist."));
      oarc << false << msg;
    } else {
      graph_row* row= v->data();
      oarc << true << *row;
      database->free_vertex(v);
    }
  }

  void graph_database_server::get_vertex_data_field(iarchive& iarc, oarchive& oarc) {
    graph_vid_t vid;
    size_t fieldpos;
    iarc >> vid >> fieldpos;
    graph_vertex* v = database->get_vertex(vid);
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ")
                         + boost::lexical_cast<std::string>(vid)
                         + std::string(". Vid does not exist."));
      oarc << false << msg;
    } else {
      graph_value* val = v->data()->get_field(fieldpos);
      oarc << true << *val;
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

    oarc << true << vid << shardid << _inadj.size() << _outadj.size();
    if (get_in) {
      for (size_t i = 0; i < _inadj.size(); i++) {
         oarc << _inadj[i]->get_src() << _inadj[i]->get_id() << *(_inadj[i]->data());
      }
      database->free_edge_vector(_inadj);
    }

    if (get_out) {
      for (size_t i = 0; i < _outadj.size(); i++) {
        oarc << _outadj[i]->get_dest() << _outadj[i]->get_id() << *(_outadj[i]->data());
      }
      database->free_edge_vector(_outadj);
    }
  }



  void graph_database_server::get_shard(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shardid;
    iarc >> shardid;
    graph_shard* shard = database->get_shard(shardid);
    if (shard == NULL) {
      std::string errormsg = ("Shard " + boost::lexical_cast<std::string>(shardid) + " does not exist");
      logstream(LOG_WARNING) << errormsg << std::endl;
      oarc << false <<  errormsg;
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
      std::string errormsg = ("Shard " + boost::lexical_cast<std::string>(shard_to) + " does not exist");
      logstream(LOG_WARNING) << errormsg << std::endl;
      oarc << false <<  errormsg;
    } else {
      oarc << true << *shard;
    }
  }


  // ----------- Modification Handlers ----------------
  void graph_database_server::set_vertex_field (iarchive& iarc,
                                                oarchive& oarc) {
    graph_vid_t vid;
    size_t fieldpos, len;
    iarc >> vid >> fieldpos >> len;
    char* val = (char*)malloc(len);
    iarc.read(val, len);

    graph_vertex* v = database->get_vertex(vid);
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ")
                         + boost::lexical_cast<std::string>(vid)
                         + std::string(". Vid does not exist."));
      oarc << false << msg;
      return;
    }

    bool success = v->data()->get_field(fieldpos)->set_val(val, len);
    v->write_changes();
    database->free_vertex(v);
    if (success) {
      oarc << success;
    } else {
      oarc << false << ("Faile to set field " + boost::lexical_cast<std::string>(fieldpos));
    }
  }

  void graph_database_server::set_vertex_row(iarchive& iarc,
                                             oarchive& oarc) {
    graph_vid_t vid;
    size_t num_changes, fieldpos, len;
    iarc >> vid >> num_changes;
    graph_vertex* v = database->get_vertex(vid);
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ")
                         + boost::lexical_cast<std::string>(vid)
                         + std::string(". Vid does not exist."));
      oarc << false << msg;
      return;
    }

    std::vector<std::string> errors;
    for (size_t i = 0; i < num_changes; i++) {
      iarc >> fieldpos >> len;
      char* val = (char*)malloc(len);
      iarc.read(val, len);
      bool success = v->data()->get_field(fieldpos)->set_val(val, len);
      if (!success)
        errors.push_back("Fail to set value for field " + boost::lexical_cast<std::string>(fieldpos));
    }
    v->write_changes();
    if (errors.size() > 0) { 
      oarc << false << boost::algorithm::join(errors, "\n");
    } else {
      oarc << true;
    }
    database->free_vertex(v);
  }

  void graph_database_server::set_edge_field(iarchive& iarc,
                                             oarchive& oarc) {
    graph_eid_t eid;
    graph_shard_id_t shardid;
    size_t fieldpos, len;
    iarc >> eid >> fieldpos >> len;
    char* val = (char*)malloc(len);
    iarc.read(val, len);
    graph_edge* e = database->get_edge(eid, shardid);
    if (e == NULL) {
      std::string msg = (std::string("Fail to get edge eid = ")
                         + boost::lexical_cast<std::string>(eid)
                         + " at shard " + boost::lexical_cast<std::string>(shardid)
                         + std::string(". eid or shardid does not exist."));
      oarc << false << msg;
    } else {
      bool success = e->data()->get_field(fieldpos)->set_val(val, len);
      e->write_changes();
      database->free_edge(e);
      oarc << success;
    }
  }

  void graph_database_server::set_edge_row(iarchive& iarc, oarchive& oarc) {
    // unimplemented
    ASSERT_TRUE(false);
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

    if (success) {
      oarc << success;
    } else {
      oarc << success << ("Fail adding vertex. Vid " 
           + boost::lexical_cast<std::string>(vid)
           + " already exists on shard "
           + boost::lexical_cast<std::string>(master));
    }
  }

  void graph_database_server::batch_add_vertex (iarchive& iarc,
                                                oarchive& oarc) {
    graph_shard_id_t master;
    size_t num_records;
    iarc >> master >> num_records;
    bool success = true;
    std::vector<std::string> error_messages;

    for (size_t i = 0; i < num_records; i++) {
      graph_vid_t vid;
      bool hasdata;
      iarc >> vid >> hasdata;
      if (hasdata) {
        graph_row* data = new graph_row;
        iarc >> *data;
        success &= database->add_vertex(vid, master, data);
      } else {
        success &= database->add_vertex(vid, master);
      }
      if (!success) {
        std::string err (("Fail adding vertex. Vid " 
                          + boost::lexical_cast<std::string>(vid)
                          + " already exists on shard "
                          + boost::lexical_cast<std::string>(master)));
        logstream(LOG_WARNING) << err << std::endl;
        error_messages.push_back(err);
      }
    }
    if (success) {
      oarc << true;
    } else {
      oarc << false << error_messages.size();
      for (size_t i = 0; i < error_messages.size(); i++) {
        oarc << error_messages[i];
      }
    }
  }

  void graph_database_server::add_edge (iarchive& iarc,
                                        oarchive& oarc) {
    graph_vid_t source, target;
    graph_shard_id_t master;
    bool hasdata;
    iarc >> source >> target >> master >> hasdata;
    if (hasdata) {
      graph_row* data = new graph_row;
      iarc >> *data;
      database->add_edge(source, target, master, data);
    } else {
      database->add_edge(source, target, master);
    }
    oarc << true;
  }

  void graph_database_server::batch_add_edge (iarchive& iarc,
                                              oarchive& oarc) {
    graph_shard_id_t master;
    size_t num_records;
    iarc >> master >> num_records;

    for (size_t i = 0; i < num_records; ++i) {
      graph_vid_t source, target;
      bool hasdata;
      iarc >> source >> target >> hasdata;
      if (hasdata) {
        graph_row* data = new graph_row;
        iarc >> *data;
        database->add_edge(source, target, master, data);
      } else {
        database->add_edge(source, target, master);
      }
    }
    oarc << true;
  }

  void graph_database_server::add_vertex_mirror (iarchive& iarc,
                                                 oarchive& oarc) {
    graph_vid_t vid;
    graph_shard_id_t master, mirror;
    size_t size;
    iarc >> vid >> master >> size;
    for (size_t i = 0; i < size; i++) {
      iarc >> mirror;
      database->add_vertex_mirror(vid, master, mirror);
    }
    oarc << true;
  }

  void graph_database_server::batch_add_vertex_mirror (iarchive& iarc,
                                                       oarchive& oarc) {
    graph_shard_id_t master;
    size_t num_records;
    iarc >> master >> num_records;
    for (size_t i = 0; i < num_records; i++) {
      graph_vid_t vid;
      size_t num_mirrors;
      graph_shard_id_t mirror;
      iarc >> vid >> num_mirrors;
      for (size_t j = 0; j <  num_mirrors; j++) {
        iarc >> mirror;
        database->add_vertex_mirror(vid, master, mirror);
      } 
    }
    oarc << true;
  }
}
