#include <graphlab/database/client/graph_client_cli.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
namespace graphlab {
  void graph_client_cli::process_get(parsed_command& cmd) {
    switch(cmd.target_type) {
     case cli_parser::GRAPH_INFO_TYPE: {
       get_graph_info();
       break;
     } // end graph_info_type
     case cli_parser::VERTEX_TYPE: {
       graph_vid_t vid;
       if (parser.parse_singleton(cmd.value, vid, "error parsing vid")) {
         get_vertex(vid);
       }
       break;
     } // end vertex_type
     case cli_parser::SHARD_TYPE:{
       graph_shard_id_t shardid;
       if (parser.parse_singleton(cmd.value, shardid, "error parsing shardid")) {
         get_shard(shardid);
       }
       break;
     } // end shard_type
     case cli_parser::VERTEX_ADJ_TYPE: {
       graph_vid_t vid;
       graph_shard_id_t shardid;
       if (parser.parse_pair(cmd.value, vid, shardid, "\t, ", "error parsing pair: vid, shardid")) {
         get_vertex_adj(vid, shardid);
       }
       break;
     } // end vertex_adj
     case cli_parser::SHARD_ADJ_TYPE: {
       graph_shard_id_t shardid;
       std::string vids_str;
       std::vector<graph_vid_t> vids;
       if (parser.parse_pair(cmd.value, shardid, vids_str, "\t ", "error parsing pair: shardid, vidstr")) {
         if (parser.parse_array(vids_str, vids, ",", "error parsing array: [vid,..]")) {
           graph_shard* shard = graph->get_shard_contents_adj_to(vids, shardid);
           if (shard != NULL) {
             std::cout << *shard << std::endl;
           }
         }
       } 
       break;
     } // end shard_adj 
     case cli_parser::VERTEX_FIELD_TYPE: {
       std::vector<graph_field> fields = graph->get_vertex_fields();
       std::cout << "vertex fields: \n";
       for (size_t i = 0; i < fields.size(); i++) {
         std::cout << fields[i];
         if (i < fields.size() - 1) 
           std::cout << " | ";
       }
       std::cout << std::endl;
       break;
     } // end vertex_field_type
     case cli_parser::EDGE_FIELD_TYPE: {
       std::vector<graph_field> fields = graph->get_edge_fields();
       std::cout << "edge fields: \n";
       for (size_t i = 0; i < fields.size(); i++) {
         std::cout << fields[i];
         if (i < fields.size() - 1) 
           std::cout << " | ";
       }
       std::cout << std::endl;
       break;
     } // end edge_field_type 
     default: error_unsupported_command();
    } // end switch
    std::cout << "done" << std::endl;
  }

  void graph_client_cli::process_set(parsed_command& cmd) {
    switch(cmd.target_type) {
     case cli_parser::VERTEX_TYPE: {
       graph_vid_t vid;
       size_t fieldpos;
       std::string val;
       if (parser.parse_triplet(cmd.value, vid, fieldpos, val, "\t, ", "error parsing triplet: vid, fieldpos, value")) {
         set_vertex(vid, fieldpos, val);
       } 
       break;
     }
     default: error_unsupported_command();
    }
    std::cout << "done" << std::endl;
  }

  void graph_client_cli::process_load(parsed_command& cmd) {
    switch(cmd.target_type) {
     case cli_parser::STRING_TYPE: {
       std::string path, format;
       if (parser.parse_pair(cmd.value, path, format, "\t, ", "error parsing pair: path, format")) {
         graph->load_format(path, format);
       }
       break;
     }
     default: error_unsupported_command();
    }
    std::cout << "done" << std::endl;
  }

  void graph_client_cli::process_batch_get(parsed_command& cmd) {
    switch(cmd.target_type) {
     case cli_parser::VERTEX_TYPE: {
       std::vector<graph_vid_t> vids;
       if (parser.parse_array(cmd.value, vids, "\t, ", "error parsing array: vids")) {
         batch_get_vertices(vids);
       }
       break;
     }
     default: error_unsupported_command();
    }
    std::cout << "done" << std::endl;
  }

  void graph_client_cli::process_add(parsed_command& cmd) {
    switch(cmd.target_type) {
     case cli_parser::VERTEX_TYPE: {
       graph_vid_t vid;
       if (parser.parse_singleton(cmd.value, vid, "error parsing singleton: vid")) {
         graph->add_vertex_now(vid);
       }
       break;
     }
     case cli_parser::EDGE_TYPE: {
       graph_vid_t src, dest;
       if (parser.parse_pair(cmd.value, src, dest, "\t, ", "error parsing singleton: vid")) {
         graph->add_edge_now(src, dest);
       }
       break;
     }
     case cli_parser::VERTEX_FIELD_TYPE: {
       std::string fieldname, fieldtype;
       if (parser.parse_pair(cmd.value, fieldname, fieldtype, ":", "error parsing pair: fieldname:fieldtype")) {
           graph_datatypes_enum type = string_to_type(fieldtype);
           if (type == UNKNOWN_TYPE) {
             logstream(LOG_ERROR) << "Unknown type: " << fieldtype << std::endl;
           } else {
             graph_field field(fieldname, type);
             graph->add_vertex_field(field);
           }
       }
       break;
     }
     case cli_parser::EDGE_FIELD_TYPE: {
       std::string fieldname, fieldtype;
       if (parser.parse_pair(cmd.value, fieldname, fieldtype, ":", "error parsing pair: fieldname:fieldtype")) {
           graph_datatypes_enum type = string_to_type(fieldtype);
           if (type == UNKNOWN_TYPE) {
             logstream(LOG_ERROR) << "Unknown type: " << fieldtype << std::endl;
           } else {
             graph_field field(fieldname, type);
             graph->add_edge_field(field);
           }
       }
       break;
     }
     default: error_unsupported_command();
    }
    std::cout << "done" << std::endl;
  }

  void graph_client_cli::process_batch_add(parsed_command& cmd) {
    switch(cmd.target_type) {
     case cli_parser::VERTEX_TYPE: {
       std::vector<graph_vid_t> vids;
       if (parser.parse_array(cmd.value, vids, "error parsing array: vids")) {
         for (size_t i = 0; i < vids.size(); i++) {
           graph->add_vertex(vids[i]);
         }
         graph->flush();
       }
       break;
     }
     default: error_unsupported_command();
    }
    std::cout << "done" << std::endl;
  }


  void graph_client_cli::get_graph_info() {
    size_t nverts = graph->num_vertices();
    size_t nedges = graph->num_edges();
    size_t nshards = graph->num_shards();
    std::cout << "Graph summary: \n" 
              << "nverts: " << nverts <<  "\n"
              << "nedges: " << nedges << "\n"
              << "nshards: " << nshards << "\n"
              << "-----------------------" << std::endl;
  }

  void graph_client_cli::get_vertex(graph_vid_t vid) {
    graph_vertex* vertex = graph->get_vertex(vid);
    if (vertex != NULL) {
      std::cout << *vertex << std::endl;
      graph->free_vertex(vertex);
    }
  }

  void graph_client_cli::get_shard(graph_shard_id_t shardid) {
    graph_shard* shard = graph->get_shard(shardid);
    if (shard != NULL) {
      std::cout << *shard << std::endl;
      graph->free_shard(shard);
    }
  }

  void graph_client_cli::get_vertex_adj(graph_vid_t vid, graph_shard_id_t shardid) {
    graph_vertex* v = graph->get_vertex(vid);
    if (v != NULL) {
      std::vector<graph_edge*> inadj;
      std::vector<graph_edge*> outadj;
      v->get_adj_list(shardid, true, &inadj, &outadj);
      std::cout << "In edges ("<< inadj.size() << "): " << "\n";
      for (size_t i = 0; i < inadj.size(); i++) {
        std::cout << *(inadj[i]);
        if (i < inadj.size()-1) 
          std::cout << "\t";
      }
      std::cout << "\n";
      std::cout << "Out edges (" << outadj.size() << "):" << "\n";
      for (size_t i = 0; i < outadj.size(); i++) {
        std::cout << *(outadj[i]);
        if (i < outadj.size()-1) 
          std::cout << "\t";
      }
      std::cout << "\n" << std::endl;
      graph->free_edge_vector(inadj);
      graph->free_edge_vector(outadj);
    }
  } 

  void graph_client_cli::set_vertex(graph_vid_t vid, size_t fieldpos, std::string& val) { 
    graph_vertex* v = graph->get_vertex(vid);
    if (v != NULL) {
        if(v->get_field(fieldpos) == NULL) {
          logstream(LOG_ERROR) << error_messages.fail_setting_value(fieldpos);
          return;
        } else {
          bool success = v->get_field(fieldpos)->set_val(val);
          if (!success) {
            logstream(LOG_ERROR) << error_messages.fail_setting_value(fieldpos);
          } else {
            v->write_changes();
          }
        }
    }
  }

  void graph_client_cli::get_shard_adj(graph_shard_id_t shardid, 
                                       std::vector<graph_vid_t>& vids) {
    graph_shard* shard = graph->get_shard_contents_adj_to(vids, shardid);
    if (shard != NULL)  {
      std::cout << *shard << std::endl;
      graph->free_shard(shard);
    }
  }

  void  graph_client_cli::batch_get_vertices(std::vector<graph_vid_t>& vids) {
    std::vector< std::vector<graph_vertex*> > vertices = graph->batch_get_vertices(vids);
    for (size_t i = 0; i < vertices.size(); i++) {
      for (size_t j = 0; j < vertices[i].size(); j++) {
        std::cout << *vertices[i][j] << std::endl;
      }
      graph->free_vertex_vector(vertices[i]);
    }
  }

  void graph_client_cli::batch_get_vertex_adj_to_shard(graph_shard_id_t shard_from,
                                                      graph_shard_id_t shard_to) {
    std::vector<graph_vertex*> vertices = graph->get_vertex_adj_to_shard(shard_from, shard_to);
    for (size_t i = 0; i < vertices.size(); i++) {
      std::cout << *(vertices[i]) << std::endl;
    }
    graph->free_vertex_vector(vertices);
  }
}// end of namespace
