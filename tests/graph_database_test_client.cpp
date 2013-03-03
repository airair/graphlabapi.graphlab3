#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/distributed_graph/distributed_graph.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <fault/query_object_server.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "graph_database_test_util.hpp"
#include <vector>
using namespace std;

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Usage: graph_database_test_client [zkhost] [prefix] \n";
    return 0;
  }

  std::string zkhost = argv[1];
  std::string prefix = argv[2];
  std::vector<std::string> zkhosts; zkhosts.push_back(zkhost);
  std::vector<std::string> server_list;
  server_list.push_back("shard0");
  server_list.push_back("shard1");
  server_list.push_back("shard2");
  server_list.push_back("shard3");

  void* zmq_ctx = zmq_ctx_new();
  graphlab::distributed_graph graph(zmq_ctx, zkhosts, prefix, server_list);

  std::vector<graphlab::graph_field> vfields = graph.get_vertex_fields();

  std::vector<graphlab::graph_field> efields = graph.get_edge_fields();

  size_t nverts = graph.num_vertices();
  size_t nedges = graph.num_edges();
  size_t nshards = graph.num_shards();

  std::cout << "Graph summary: \n" 
            << "nverts: " << nverts <<  "\n"
            << "nedges: " << nedges << "\n"
            << "nshards: " << nshards << "\n"
            << "vertex fields: ";
  for (size_t i = 0; i < vfields.size(); i++) {
    std::cout << vfields[i] << "\t"; 
  }
  std::cout << "\n" << "edge fields: ";
  for (size_t i = 0; i < efields.size(); i++) {
    std::cout << efields[i] << "\t";
  }
  std::cout << "\n" << "-----------------------" << std::endl;

  while(1) {
    std::string cmd, target, val;
    std::cin >> cmd;
    if (cmd == "q") break;

    std::cin >> target;
    getline(std::cin, val);
    boost::algorithm::trim(val);

    // get methods
    if (cmd == "get") { 
      if (target == "shard") {
        graphlab::graph_shard_id_t shardid;
        try {
          shardid = boost::lexical_cast<graphlab::graph_shard_id_t>(val);
        } catch (boost::bad_lexical_cast &){
          std::cout << val  << " cannot be converted into shard id" << std::endl;
          continue;
        }
        std::cout << "Trying to get shard " << shardid << std::endl;
        graphlab::graph_shard* shard = graph.get_shard(shardid);
        if (shard) {
          std::cout << "Succeed in getting shard " << shardid << ":\n"
                    << *shard << std::endl;
          graph.free_shard(shard);
        } else {
          std::cout << "Fail getting shard: " << shardid << std::endl;
        }
      } else  if (target == "vertex") {
        graphlab::graph_vid_t vid;
        try {
         vid = boost::lexical_cast<graphlab::graph_vid_t>(val);
        } catch (boost::bad_lexical_cast &) {
         std::cout << val  << " cannot be converted into vertex id" << std::endl;
         continue;
        }
        std::cout << "Trying to get vertex " << vid << std::endl;
        graphlab::graph_vertex * v = graph.get_vertex(vid);
        if (v != NULL)
          std::cout << *v << std::endl;
      } else if (target == "vertex_adj") { 
        std::cout << "Trying to get vertex adjacency list: " << val << std::endl;
        graphlab::graph_vid_t vid;
        graphlab::graph_shard_id_t shardid;
        try {
         std::vector<std::string> strs;
         boost::split(strs, val, boost::is_any_of("\t ,"));
         if (strs.size() != 2) {
          std::cout << val << " cannot be converted to (vid shardid) pair" << std::endl;
          continue;
         }
         vid = boost::lexical_cast<graphlab::graph_vid_t>(strs[0]);
         shardid = boost::lexical_cast<graphlab::graph_shard_id_t>(strs[1]);
        } catch (boost::bad_lexical_cast &) {
          std::cout << val << " cannot be converted to (vid shardid) pair" << std::endl;
          continue;
        }
        graphlab::graph_vertex* v = graph.get_vertex(vid);
        if (v != NULL) {
          std::vector<graphlab::graph_edge*> inadj;
          std::vector<graphlab::graph_edge*> outadj;
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
        }
      } else {
        std::cout << "Unknown get target: " << target << " " << val << std::endl;
      }
    // ingress methods 
    } else if (cmd == "add") {
      if (target == "vertex") {
        graphlab::graph_vid_t vid;
        try {
          vid = boost::lexical_cast<graphlab::graph_vid_t>(val);
        } catch (boost::bad_lexical_cast &) {
          std::cout << val << " cannot be converted into vertex id" << std::endl;
          continue;
        }
        std::cout << "Trying to add vertex " << vid << std::endl;
        graph.add_vertex_now(vid);
        std::cout << "Done" << std::endl;
      } else if (target == "edge") {
        std::cout << "Trying to add edge " << val << std::endl;
        std::vector<std::string> strs;
        boost::split(strs, val, boost::is_any_of("\t ,"));
        if (strs.size() != 2) {
          std::cout << val << " cannot be converted to (src dest) pair" << std::endl;
          continue;
        }
        graphlab::graph_vid_t src, dest;
        try {
          src = boost::lexical_cast<graphlab::graph_vid_t>(strs[0]);
          dest = boost::lexical_cast<graphlab::graph_vid_t>(strs[1]);
        } catch (boost::bad_lexical_cast &) {
          std::cout << val << " cannot be converted to (src dest) pair" << std::endl;
          continue;
        }
        graph.add_edge_now(src, dest);
        std::cout << "Done" << std::endl;
      } else {
        std::cout << "Unknown add target: " << target << " " << val << std::endl;
      }
    // set methods 
    } else if (cmd == "set") {
      if (target == "vertex") {
        std::cout << "Trying to set vertex " << val << std::endl;
        std::cout << "not implemented" << std::endl;
      } else if (target == "edge") {
        std::cout << "Trying to set edge " << val << std::endl;
        std::cout << "not implemented" << std::endl;
      } else {
        std::cout << "Unknown set target: " << target << " " << val << std::endl;
      }
    } else if (cmd == "load") {
      std::cout << "Trying to load " << target << " format=" << val << std::endl;
      graph.load_format(target, val);
      std::cout << "Done" << std::endl;
    } else {
        std::cout << "Unknown command: " << cmd << " " << target << " " << val << std::endl;
    }
    std::cout << std::endl;
  }
}
