#include<graphlab/database/engine/graph_database_synchronous_engine.hpp>
#include<graphlab/database/graph_database.hpp>
#include<graphlab/database/graph_shard_manager.hpp>
#include<graphlab/database/client/graph_client.hpp>
#include<graphlab/database/client/graph_vertex_remote.hpp>
namespace graphlab {
    /*
     * initalize the engine. Initializing vertex states and
     * remote vertices that are adjacent to the local shards.
     */
    void graph_database_synchronous_engine::init() {
      logstream(LOG_EMPH) << "Engine init..." << std::endl;

      // Initialize local and remote sharding list
      local_shards = database->get_shard_list();
      for (size_t i = 0; i < query_client->num_shards(); i++) {
        if (database->get_shard(i) == NULL)
          remote_shards.push_back(i);
      }

      size_t pos = 0;
      // Gather local vertices
      for (size_t i = 0; i < local_shards.size(); i++) {
        graph_shard* shard = database->get_shard(local_shards[i]);
        for (size_t j = 0; j < shard->num_vertices(); j++) {
          graph_vid_t vid = shard->vertex(j);
          vid2lvid[vid] = pos++;
          vertex_array.push_back(database->get_vertex(vid, shard->id()));
        }
      }
      num_local_vertices = pos;
      // ASSERT_EQ(num_local_vertices, database->num_vertices);

      // Gather remote adjacent vertices
      for (size_t i = 0; i < local_shards.size(); i++) {
        for (size_t j = 0; j < remote_shards.size(); j++) {
          std::vector<graph_vertex*> vertices = 
              query_client->get_vertex_adj_to_shard(remote_shards[j], local_shards[i]);

          if (vertices.size() > 0) {
            for (size_t k = 0; k < vertices.size(); k++) {
              vid2lvid[vertices[k]->get_id()]=pos++;
            }
            vertex_array.insert(vertex_array.end(), vertices.begin(), vertices.end());
            remote_vertex_vector_head.push_back((graph_vertex_remote*)(vertices[0]));
          }
        }
      }
      vstates.resize(pos);
    }

    /*
     * Run the computation synchrounously for niter iterations.
     */
    void graph_database_synchronous_engine::run() {
      logstream(LOG_EMPH) << "Engine run..." << std::endl;
      for (size_t i = 0; i < local_shards.size(); i++) {
        graph_shard* shard = database->get_shard(local_shards[i]);
        for (size_t j = 0; j < shard->num_edges(); j++) {
          std::pair<graph_vid_t, graph_vid_t> e = shard->edge(j); 
          graph_vid_t source = e.first;
          size_t num_out_edges = query_client->num_out_edges(source);
          shard->edge_data(j)->get_field(0)->set_double((double)num_out_edges);
        }
      }

      std::vector<query_result> reply_queue; 

      for (size_t i = 0; i < vertex_array.size(); i++) {
        vertex_array[i]->refresh();
      }

      // Gather: iterate over all edges
      for (size_t i = 0; i < local_shards.size(); i++) {
        graph_shard* shard = database->get_shard(local_shards[i]);
        for (size_t j = 0; j < shard->num_edges(); j++) {
          std::pair<graph_vid_t, graph_vid_t> e = shard->edge(j); 
          graph_vid_t source = e.first;
          graph_vid_t dest = e.second;
          size_t src_id = vid2lvid[source];
          size_t dest_id = vid2lvid[dest];
          // double pr = 0.0;
          double w = 0.0;
          // vertex_array[src_id]->immutable_data()->get_field(0)->get_double(&pr);
          shard->edge_data(j)->get_field(0)->get_double(&w);
          vstates[dest_id].acc += w;
        }
      }
    
      // Apply
      for (size_t i = 0; i < vertex_array.size(); i++) {
          graph_value value(DOUBLE_TYPE);
          value.set_double(vstates[i].acc);
          // local vertex do not use delta commit, need to guarentee local changes applies before remote changes
          if (i < num_local_vertices)  
            write_vertex_changes(i, value, reply_queue, true);
          else  // remote vertex use delta commit
            write_vertex_changes(i, value, reply_queue, true);
      } 
      for (size_t i = 0; i < reply_queue.size(); i++) {
        std::string errormsg;
        bool success = messages.parse_reply(reply_queue[i].get_reply(), errormsg);
        if (!success)
          return;
      }

      //  Clearing the partial accumulates. 
      for (size_t i = 0; i < vstates.size(); i++) {
        vstates[i].acc = 0;
      }
      reply_queue.clear();

      // Scatter
    }

    /*
     * Finalize the engine. Free the resources.
     */
    void graph_database_synchronous_engine::finalize() {
      logstream(LOG_EMPH) << "Engine finalize..." << std::endl;
      for (size_t i = 0; i < remote_vertex_vector_head.size(); i++) {
        delete[] remote_vertex_vector_head[i]; // equivalent to query_client->free_vertex_vector
      }
      for (size_t i = 0; i < num_local_vertices; i++) {
        database->free_vertex(vertex_array[i]);
      }
      vstates.clear();
      vertex_array.clear();
      remote_vertex_vector_head.clear();
      vid2lvid.clear();
    }


    void graph_database_synchronous_engine::write_vertex_changes(size_t lvid, const graph_value& value,
                                                                 std::vector<query_result>& reply_queue,
                                                                 bool use_delta) {
      // write locally
      graph_vid_t vid = vertex_array[lvid]->get_id();

      logstream(LOG_EMPH) << "Write " << value << " to vertex " << vid << " use_deltla = " << use_delta << std::endl;
      graph_shard_id_t master = query_client->get_shard_manager().get_master(vid);
      if (lvid < num_local_vertices) {
        graph_row* row = database->get_shard(master)->vertex_data_by_id(vid);
        database->set_field(row, 0, value, use_delta);
      } else{ 
      // write remotely 
          int len;
          char* msg = messages.update_vertex_field_request(&len, vid, master,
                                                           0, &value, use_delta);
          query_client->query_async(query_client->find_server(master), msg, len, reply_queue);
      }
    }
}// end of namespace
