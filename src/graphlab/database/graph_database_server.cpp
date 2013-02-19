#include<graphlab/database/graph_database_server.hpp>

namespace graphlab {

  std::string graph_database_server::get_vertex_fields() {
    const std::vector<graph_field>& fields = database->get_vertex_fields();
    oarchive oarc;
    oarc << true << fields;
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }

  std::string graph_database_server::get_edge_fields() {
    const std::vector<graph_field>& fields = database->get_edge_fields();
    oarchive oarc;
    oarc << true << fields;
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    return ret;
  }

  std::string graph_database_server::get_vertex_data(graph_vid_t vid) {
    graph_vertex* v = database->get_vertex(vid);
    oarchive oarc;
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ") + boost::lexical_cast<std::string>(vid) + std::string(". Vid does not exist."));
      return error_msg(msg);
    } else {
      graph_row* row= v->data();
      oarc << true << *row;
      std::string ret(oarc.buf, oarc.off);
      free(oarc.buf);
      database->free_vertex(v);
      return ret;
    }
  }

  std::string graph_database_server::get_vertex_data(graph_vid_t vid, size_t fieldpos) {
    graph_vertex* v = database->get_vertex(vid);
    oarchive oarc;
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ")
                         + boost::lexical_cast<std::string>(vid)
                         + std::string(". Vid does not exist."));
      return error_msg(msg);
    } else {
      graph_value* val = v->data()->get_field(fieldpos);
      oarc << true << *val;
      std::string ret(oarc.buf, oarc.off);
      free(oarc.buf);
      database->free_vertex(v);
      return ret;
    }
  }

  std::string graph_database_server::get_vertex_adj(graph_vid_t vid,
                                                   graph_shard_id_t shardid,
                                                   bool get_in,
                                                   bool get_out,
                                                   bool prefetch_data) {
    graph_vertex* v = this->database->get_vertex(vid);
    oarchive oarc;
    if (v == NULL) {
      std::string msg= (std::string("Fail to get vertex id = ")
                        + boost::lexical_cast<std::string>(vid)
                        + std::string(". Vid does not exist."));
      return error_msg(msg);
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
    std::string ret(oarc.buf, oarc.off);
    free(oarc.buf);
    database->free_vertex(v);
    return ret;
  }



  // ----------- Modification Handlers ----------------

  std::string graph_database_server::set_vertex_field (graph_vid_t vid,
                                                       size_t fieldpos,
                                                       const char* val,
                                                       size_t len) {
    graph_vertex* v = database->get_vertex(vid);
    if (v == NULL) {
      std::string msg = (std::string("Fail to get vertex id = ")
                         + boost::lexical_cast<std::string>(vid)
                         + std::string(". Vid does not exist."));
      return error_msg(msg);
    } else {
      bool success = v->data()->get_field(fieldpos)->set_val(val, len);
      v->write_changes();
      database->free_vertex(v);

      oarchive oarc;
      oarc << success;
      std::string ret(oarc.buf, oarc.off);
      free(oarc.buf);
      return ret;
    }
  }

  std::string graph_database_server::set_edge_field(graph_eid_t eid,
                                                    graph_shard_id_t shardid,
                                                    size_t fieldpos,
                                                    const char* val, size_t len) {
    std::string ret;
    graph_edge* e = database->get_edge(eid, shardid);
    if (e == NULL) {
      std::string msg = (std::string("Fail to get edge eid = ")
                         + boost::lexical_cast<std::string>(eid)
                         + " at shard " + boost::lexical_cast<std::string>(shardid)
                         + std::string(". eid or shardid does not exist."));
      return error_msg(msg);
    } else {
      bool success = e->data()->get_field(fieldpos)->set_val(val, len);
      e->write_changes();
      database->free_edge(e);

      oarchive oarc;
      oarc << success;
      std::string ret(oarc.buf, oarc.off);
      free(oarc.buf);
      return ret;
    }
  }
}
