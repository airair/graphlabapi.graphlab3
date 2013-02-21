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
    bool get_in, get_out, prefetch_data;
    iarc >> vid >> shardid >> get_in >> get_out >> prefetch_data;
    graph_vertex* v = this->database->get_vertex(vid);
    if (v == NULL) {
      std::string msg= (std::string("Fail to get vertex id = ")
                        + boost::lexical_cast<std::string>(vid)
                        + std::string(". Vid does not exist."));
      oarc << false << msg;
    } else {
      std::vector<graph_edge*> _inadj;
      std::vector<graph_edge*> _outadj;
      std::vector<graph_edge*>* out_inadj = get_in ? &(_inadj) : NULL;
      std::vector<graph_edge*>* out_outadj = get_out ? &(_outadj) : NULL;

      v->get_adj_list(shardid, prefetch_data, out_inadj, out_outadj);

      size_t numin = get_in ? out_inadj->size() : 0;
      size_t numout = get_out ? out_outadj->size() : 0;
      oarc << true << numin << numout << prefetch_data;
      for (size_t i = 0; i < numin; i++) {
        graph_edge* e = out_inadj->at(i);
        oarc << e->get_src() << e->get_id();
        if (prefetch_data)
          oarc << *(e->data());
      }
      for (size_t i = 0; i < numout; i++) {
        graph_edge* e = out_outadj->at(i);
        oarc << e->get_dest() << e->get_id();
        if (prefetch_data)
          oarc << *(e->data());
      }
      if (out_inadj) database->free_edge_vector(out_inadj);
      if (out_outadj) database->free_edge_vector(out_outadj);
    }
    database->free_vertex(v);
  }

  void graph_database_server::get_shard(iarchive& iarc, oarchive& oarc) {
    graph_shard_id_t shardid;
    iarc >> shardid;
    graph_shard* shard = database->get_shard(shardid);
    ASSERT_TRUE(shard != NULL);
    oarc << true << *shard;
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
    } else {
      bool success = v->data()->get_field(fieldpos)->set_val(val, len);
      v->write_changes();
      database->free_vertex(v);
      oarchive oarc;
      oarc << success;
    }
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
}
