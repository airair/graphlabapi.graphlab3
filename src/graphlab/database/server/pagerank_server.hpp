#ifndef GRAPHLAB_DATABASE_PAGERANK_SERVER_HPP
#define GRAPHLAB_DATABASE_PAGERANK_SERVER_HPP
#include<graphlab/database/server/graph_database_server.hpp>
#include<graphlab/database/graph_database.hpp>

namespace graphlab {

  class pagerank_server {
   public:
     /**
      * States for vertex programs
      */
     struct vertex_state{
       double acc;
       bool is_scheduled;
     };

     boost::unordered_map<graph_vid_t, size_t> vid2index;

     std::vector<vertex_state> vstates; 

     boost::unordered_map<graph_shard_id_t, std::vector<graph_vertex*> > remote_vertices;

     std::vector<graph_vertex*> vertex_view; 

     std::vector<graph_shard_id_t> shard_list;

   public:
     pagerank_server(graph_database_server* server) : 
         server(server),
        database(server->get_database()),
        query_client(server->get_query_client()) {
     }

     ~pagerank_server() { }

     /**
      * Handle the SET queries.
      * This function does not free the request message.
      */
     std::string update(char* request, size_t len) {
       std::string header;
       iarchive iarc(request, len);
       iarc >> header;
       logstream(LOG_EMPH) << "update: " << header << std::endl;
         return server->update(request, len);
     } 

     /**
      * Handle the GET queries.
      * This function does not free the request message.
      */
     std::string query(const char* request, size_t len) {
       std::string header, cmd;
       iarchive iarc(request, len);
       iarc >> header;
       logstream(LOG_EMPH) << "query: " << header << std::endl;
       // if (header == "pagerank") {
       //   iarc >> cmd;
       //   if (cmd == "init") {
       //     initialize();
       //   } else if (cmd == "run") {
       //     run_iter();
       //   } else if (cmd == "finalize") {
       //     finalize();
       //   } else {
       //     logstream(LOG_ERROR) << "unknown command: "
       //                          << header << " " << cmd << std::endl;
       //   }
       //   oarchive oarc;
       //   oarc << true; 
       //   std::string ret(oarc.buf, oarc.off);
       //   free(oarc.buf);
       //   return ret;
       // } else {
        return server->query(request, len);
       // }
     }


    // void initialize() {
    //   graph_database* database = server->get_database();
    //   shard_list = server->get_database()->get_shard_list();
    //   size_t pos = 0;
    //   logstream(LOG_EMPH) << "pagerank initializing..." << std::endl;
    //   logstream(LOG_EMPH) << "set local vertex to 0... "<< std::endl;
    //   
    //   // Iterate over local vertices, and initialize to 0
    //   for (size_t i = 0; i < shard_list.size(); i++) {
    //     graph_shard* shard = database->get_shard(shard_list[i]);
    //     for (size_t j = 0; j < shard->num_vertices(); j++) {
    //       graph_vid_t vid = shard->vertex(j);
    //       graph_row* vdata = shard->vertex_data(j);
    //       vdata->get_field(0)->set_double(0.0);
    //       vid2index[vid] = pos++;
    //       vertex_view.push_back(database->get_vertex(vid, shard->id()));
    //     }
    //   }

    //   // barrier... 
    //   // Get dependent remote vetices
    //   logstream(LOG_EMPH) << "get remote adj vertices... "<< std::endl;
    //   for (size_t i = 0; i < shard_list.size(); i++) {
    //     for (size_t j = 0; j < query_client->num_shards(); j++) {
    //       if (database->get_shard(j) != NULL)
    //         continue;

    //       logstream(LOG_EMPH) << "send adj vertices query... "<< std::endl;
    //       std::vector<graph_vertex*> vertices = query_client->get_vertex_adj_to_shard(j, shard_list[i]);
    //       logstream(LOG_EMPH) << "received adj vertices reply... "<< std::endl;

    //       for (size_t k = 0; k < vertices.size(); k++) {
    //         vid2index[vertices[k]->get_id()]=pos++;
    //       }
    //       remote_vertices[j] = vertices;
    //       vertex_view.insert(vertex_view.end(), vertices.begin(), vertices.end());
    //     }
    //   }

    //   vstates.resize(pos);

    //   logstream(LOG_EMPH) << "count out edges... "<< std::endl;
    //   // count out edges
    //   for (size_t i = 0; i < shard_list.size(); i++) {
    //     graph_shard* shard = database->get_shard(shard_list[i]);
    //     for (size_t j = 0; j < shard->num_edges(); j++) {
    //       std::pair<graph_vid_t, graph_vid_t> e = shard->edge(j); 
    //       size_t src_id = vid2index[e.first];
    //       vstates[src_id].acc += 1;
    //     }
    //   }

    //   logstream(LOG_EMPH) << "transform out edges... "<< std::endl;
    //   // transform out edges
    //   for (size_t i = 0; i < shard_list.size(); i++) {
    //     graph_shard* shard = database->get_shard(shard_list[i]);
    //     for (size_t j = 0; j < shard->num_edges(); j++) {
    //       std::pair<graph_vid_t, graph_vid_t> e = shard->edge(j); 
    //       size_t src_id = vid2index[e.first];
    //       double num_out = vstates[src_id].acc; 
    //       graph_value* edgedata = shard->edge_data(j)->get_field(0);
    //       edgedata->set_double(1.0/num_out);
    //     }
    //     database->commit_shard(shard);
    //   }

    //   logstream(LOG_EMPH) << "clean up vertex states... "<< std::endl;
    //   // clean up vertex states
    //   for (size_t i = 0; i < vstates.size(); i++) {
    //     vstates[i].acc = 0;
    //   }
    // }

    // void run_iter() {
    //   graph_database* database = server->get_database();
    //   shard_list = server->get_database()->get_shard_list();


    //   logstream(LOG_EMPH) << "gather... " << std::endl;
    //   // gather
    //   for (size_t i = 0; i < shard_list.size(); i++) {
    //     graph_shard* shard = database->get_shard(shard_list[i]);
    //     for (size_t j = 0; j < shard->num_edges(); j++) {
    //       std::pair<graph_vid_t, graph_vid_t> e = shard->edge(j);
    //       graph_row* edata = shard->edge_data(j);

    //       size_t srcid = vid2index[e.first];
    //       size_t destid = vid2index[e.second];
    //       graph_double_t weight;
    //       graph_double_t pr;

    //       edata->get_field(0)->get_double(&weight);
    //       vertex_view[srcid]->data()->get_field(0)->get_double(&pr);
    //       vstates[destid].acc += weight*pr;
    //     }
    //   }

    //   logstream(LOG_EMPH) << "apply... " << std::endl;
    //   // apply
    //   for (size_t i = 0; i < vertex_view.size(); i++) {
    //     graph_double_t old;
    //     vertex_view[i]->data()->get_field(0)->get_double(&old);
    //     vertex_view[i]->data()->get_field(0)->set_double(old + vstates[i].acc);
    //     vertex_view[i]->write_changes();
    //   }
    //   

    //   logstream(LOG_EMPH) << "scatter... " << std::endl;
    //   // scatter
    //   for (size_t i = 0; i < shard_list.size(); i++) {
    //     graph_shard* shard = database->get_shard(shard_list[i]);
    //     for (size_t j = 0; j < shard->num_edges(); j++) {
    //       // nothing to do
    //     }
    //   }

    //   // clean up vertex states
    //   for (size_t i = 0; i < vstates.size(); i++) {
    //     vstates[i].acc = 0;
    //   }
    // }

    // void finalize() {
    //   for (size_t i = 0; i < remote_vertices.size(); i++) { 
    //     query_client->free_vertex_vector(remote_vertices[i]);
    //   }
    //   // for (size_t i = 0; i < local_vertices.size(); i++) {
    //   //   server->get_database->free_vertex(local_vertices[i]);
    //   // }
    // }
   private:
    graph_database_server* server;
    graph_database* database;
    graph_client* query_client;
  };
}
#endif
