#include <graphlab/database/server/graphdb_server.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>

namespace graphlab {
     typedef graph_database::vertex_adj_descriptor vertex_adj_descriptor;
     typedef graph_database::vertex_insert_descriptor vertex_insert_descriptor;
     typedef graph_database::edge_insert_descriptor edge_insert_descriptor;
     typedef graph_database::mirror_insert_descriptor mirror_insert_descriptor;

  // ------------------ Server Query and Update interface ----------------------------
  bool graphdb_server::update(char* msg, size_t msglen, char** outreply, size_t *outreplylen) {
    logstream(LOG_EMPH) << "Update Request. "; 
    oarchive oarc;
    bool success = process(msg, msglen, oarc);
    if (success) {
      logstream(LOG_EMPH) << "Success." << std::endl;
    } else {
      logstream(LOG_WARNING) << "Failure." << std::endl; 
    }
    *outreply = oarc.buf;
    *outreplylen = oarc.off;
    return success; 
  }

  void graphdb_server::query(char* msg, size_t msglen, char** outreply, size_t *outreplylen) {
    logstream(LOG_EMPH) << "Query Request. "; 
    oarchive oarc;
    bool success = process(msg, msglen, oarc);
    if (success) {
      logstream(LOG_EMPH) << "Success." << std::endl;
    } else {
      logstream(LOG_WARNING) << "Failure." << std::endl; 
    }
    *outreply = oarc.buf;
    *outreplylen = oarc.off;
  }

  bool graphdb_server::process(char* msg, size_t msglen, oarchive& oarc) {
    QueryMessage qm(msg, msglen);
    QueryMessage::header header = qm.get_header();
    logstream(LOG_EMPH) << header << std::endl;

    switch (header.cmd) {
     case QueryMessage::GET: return (process_get(qm, oarc) == 0);
     case QueryMessage::SET: return (process_set(qm, oarc) == 0);
     case QueryMessage::ADD: return (process_add(qm, oarc) == 0);
     case QueryMessage::BADD: return process_batch_add(qm, oarc);
     case QueryMessage::BGET: return process_batch_get(qm, oarc);
     case QueryMessage::BSET: return process_batch_set(qm, oarc);
     case QueryMessage::ADMIN: return (process_admin(qm, oarc) == 0);
     default: return false;
    }
  }

  // ------------------ Handler for add/get/set request ----------------------------
  int graphdb_server::process_add(QueryMessage& qm, oarchive& oarc) {
    int errorcode = 0;
    QueryMessage::header h = qm.get_header(); 
    switch (h.obj) {
     case QueryMessage::VERTEX: {
       graph_vid_t vid; graph_row data;
       qm >> vid >> data;
       errorcode = server.add_vertex(vid, data);
       break;
     }
     case QueryMessage::EDGE: {
       graph_vid_t source, dest; graph_row data;
       qm >> source >> dest >> data;
       errorcode = server.add_edge(source, dest, data);
       break;
     }
     case QueryMessage::VMIRROR: {
       graph_vid_t vid;  std::vector<graph_shard_id_t> mirrors;
       qm >> vid >> mirrors;
       errorcode = server.add_vertex_mirror(vid, mirrors);
       break;
     }
     case QueryMessage::VFIELD: {
       graph_field field;
       qm >> field;
       errorcode = server.add_vertex_field(field);
       break;
     }
     case QueryMessage::EFIELD: {
       graph_field field;
       qm >> field;
       errorcode = server.add_edge_field(field);
       break;
     }
     default: errorcode = EINVHEAD;
    }
    oarc << errorcode;
    return errorcode;
  }

  int graphdb_server::process_get(QueryMessage& qm, oarchive& oarc) {
    int errorcode = 0;
    QueryMessage::header h = qm.get_header(); 
    switch (h.obj) {
     case QueryMessage::VERTEX: {
       graph_vid_t vid; 
       qm >> vid; 
       graph_row data;
       errorcode = server.get_vertex(vid, data);
       oarc << errorcode;
       if (errorcode == 0) oarc << data;
       break;
     }
     case QueryMessage::EDGE: {
       graph_eid_t eid;
       qm >> eid; 
       graph_row data;
       errorcode = server.get_edge(eid, data);
       oarc << errorcode;
       if (errorcode == 0) oarc << data;
       break;
     }
     case QueryMessage::VERTEXADJ: {
       graph_vid_t vid; bool in_edges;
       qm >> vid >> in_edges;
       vertex_adj_descriptor data;
       errorcode = server.get_vertex_adj(vid, in_edges, data);
       oarc << errorcode;
       if (errorcode == 0) oarc << data;
       break;
     }
     case QueryMessage::VFIELD: {
       errorcode = 0;
       oarc << 0 << (server.get_vertex_fields());
       break;
     }
     case QueryMessage::EFIELD: {
       errorcode = 0;
       oarc << 0 << (server.get_edge_fields());
       break;
     }
     case QueryMessage::NVERTS: {
        errorcode = 0;
        oarc << 0 << (server.num_vertices());
        break;
      } 
     case QueryMessage::NEDGES: {
        errorcode = 0;
        oarc << 0 <<  (server.num_edges());
        break;
      }
     default: errorcode = EINVHEAD;
              oarc << errorcode;
    }
    return errorcode;
  }

  int graphdb_server::process_set(QueryMessage& qm, oarchive& oarc) {
    int errorcode = 0;
    QueryMessage::header h = qm.get_header();
    switch (h.obj) {
     case QueryMessage::VERTEX: {
       graph_vid_t vid; graph_row data;
       qm >> vid >> data;
       errorcode = server.set_vertex(vid, data);
       break;
     }
     case QueryMessage::EDGE: {
       graph_eid_t eid; graph_row data;
       qm >> eid >> data;
       errorcode = server.set_edge(eid, data);
       break;
     }
     default: errorcode = EINVHEAD;
    }
    oarc << errorcode;
    return errorcode;
  }

  // ------------------ Handler for batch add/get/set request ----------------------------
  bool graphdb_server::process_batch_add(QueryMessage& qm, oarchive& oarc) {
    bool success = false;
    std::vector<int> errorcodes;
    QueryMessage::header h = qm.get_header();
    switch (h.obj) {
     case QueryMessage::VERTEX:  {
       std::vector<vertex_insert_descriptor> in;
       qm >> in;
       success = server.add_vertices(in, errorcodes);
       break;
     }
     case QueryMessage::EDGE: {
       std::vector<edge_insert_descriptor> in;
       qm >> in;
       success = server.add_edges(in, errorcodes);
       break;
     }
     case QueryMessage::VMIRROR: {
       std::vector<mirror_insert_descriptor> in;
       qm >> in;
       success = server.add_vertex_mirrors(in, errorcodes);
       break;
     }
     default: oarc << false << EINVHEAD; 
              return false;
    }
    oarc << success;
    if (!success)
      oarc << errorcodes;

    return success;
  }

  bool graphdb_server::process_batch_get(QueryMessage& qm, oarchive& oarc) {
    bool success = false;
    std::vector<int> errorcodes;
    QueryMessage::header h = qm.get_header();
    switch (h.obj) {
     case QueryMessage::VERTEX:  {
       std::vector<graph_vid_t> in;
       std::vector<graph_row> out;
       qm >> in;
       success = server.get_vertices(in, out, errorcodes);
       oarc << success << out;
       break;
     }
     case QueryMessage::EDGE: {
       std::vector<graph_eid_t> in;
       std::vector<graph_row> out;
       qm >> in;
       success = server.get_edges(in, out, errorcodes);
       oarc << success << out;
       break;
     }
     default: oarc << false << EINVHEAD; 
              return false;
    }
    if (!success)
      oarc << errorcodes;
    return success;
  }

  bool graphdb_server::process_batch_set(QueryMessage& qm, oarchive& oarc) {
    bool success = false;
    std::vector<int> errorcodes;
    QueryMessage::header h = qm.get_header();
    switch (h.obj) {
     case QueryMessage::VERTEX:  {
       std::vector< std::pair<graph_vid_t, graph_row> > in;
       qm >> in;
       success = server.set_vertices(in, errorcodes);
       break;
     }
     case QueryMessage::EDGE: {
       std::vector< std::pair<graph_eid_t, graph_row> > in;
       qm >> in;
       success = server.set_edges(in, errorcodes);
       break;
     }
     default: oarc << false << EINVHEAD; 
              return false;
    }
    oarc << success;
    if (!success) {
      oarc << errorcodes;
    }
    return success;
  }

  int graphdb_server::process_admin(QueryMessage& qm, oarchive& oarc) {
    switch (qm.get_header().obj) {
      case QueryMessage::RESET:
        server.clear();
        return 0;
      default:
        oarc << false << EINVHEAD; 
        return EINVHEAD;
    }
  }
} // end of namespace
