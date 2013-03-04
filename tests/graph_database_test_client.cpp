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
    cout << "Usage: graph_database_test_client [zkhost] [prefix] \n";
    return 0;
  }

  string zkhost = argv[1];
  string prefix = argv[2];
  vector<string> zkhosts; zkhosts.push_back(zkhost);
  vector<string> server_list;
  server_list.push_back("shard0");
  server_list.push_back("shard1");
  server_list.push_back("shard2");
  server_list.push_back("shard3");

  void* zmq_ctx = zmq_ctx_new();
  graphlab::distributed_graph graph(zmq_ctx, zkhosts, prefix, server_list);

  vector<graphlab::graph_field> vfields = graph.get_vertex_fields();

  vector<graphlab::graph_field> efields = graph.get_edge_fields();

  size_t nverts = graph.num_vertices();
  size_t nedges = graph.num_edges();
  size_t nshards = graph.num_shards();

  cout << "Graph summary: \n" 
            << "nverts: " << nverts <<  "\n"
            << "nedges: " << nedges << "\n"
            << "nshards: " << nshards << "\n"
            << "vertex fields: ";
  for (size_t i = 0; i < vfields.size(); i++) {
    cout << vfields[i] << "\t"; 
  }
  cout << "\n" << "edge fields: ";
  for (size_t i = 0; i < efields.size(); i++) {
    cout << efields[i] << "\t";
  }
  cout << "\n" << "-----------------------" << endl;

  while(1) {
    string cmd, target, val;
    cin >> cmd;
    if (cmd == "q") break;
    cin >> target;
    getline(cin, val);
    boost::algorithm::trim(val);
    //------------------------ GET commands ----------------------
    if (cmd == "get") { 
      if (target == "shard") { // get shard shardid
        graphlab::graph_shard_id_t shardid;
        try {
          shardid = boost::lexical_cast<graphlab::graph_shard_id_t>(val);
        } catch (boost::bad_lexical_cast &){
          cout << val  << " cannot be converted into shard id" << endl;
          continue;
        }
        cout << "Trying to get shard " << shardid << endl;
        graphlab::graph_shard* shard = graph.get_shard(shardid);
        if (shard) {
          cout << *shard << endl;
          graph.free_shard(shard);
        } else {
          cout << "Fail getting shard: " << shardid << endl;
        }
      } else  if (target == "vertex") { // get vertex vid
        graphlab::graph_vid_t vid;
        try {
         vid = boost::lexical_cast<graphlab::graph_vid_t>(val);
        } catch (boost::bad_lexical_cast &) {
         cout << val  << " cannot be converted into vertex id" << endl;
         continue;
        }
        cout << "Trying to get vertex " << vid << endl;
        graphlab::graph_vertex * v = graph.get_vertex(vid);
        if (v != NULL)
          cout << *v << endl;
        graph.free_vertex(v);
      } else if (target == "vertex_adj") {  // get vertex adj vid shardid 
        cout << "Trying to get vertex adjacency list: " << val << endl;
        graphlab::graph_vid_t vid;
        graphlab::graph_shard_id_t shardid;
        try {
         vector<string> strs;
         boost::split(strs, val, boost::is_any_of("\t ,"));
         if (strs.size() != 2) {
          cout << val << " cannot be converted to (vid shardid) pair" << endl;
          continue;
         }
         vid = boost::lexical_cast<graphlab::graph_vid_t>(strs[0]);
         shardid = boost::lexical_cast<graphlab::graph_shard_id_t>(strs[1]);
        } catch (boost::bad_lexical_cast &) {
          cout << val << " cannot be converted to (vid shardid) pair" << endl;
          continue;
        }
        graphlab::graph_vertex* v = graph.get_vertex(vid);
        if (v != NULL) {
          vector<graphlab::graph_edge*> inadj;
          vector<graphlab::graph_edge*> outadj;
          v->get_adj_list(shardid, true, &inadj, &outadj);
          cout << "In edges ("<< inadj.size() << "): " << "\n";
          for (size_t i = 0; i < inadj.size(); i++) {
            cout << *(inadj[i]);
            if (i < inadj.size()-1) 
              cout << "\t";
          }
          cout << "\n";
          cout << "Out edges (" << outadj.size() << "):" << "\n";
          for (size_t i = 0; i < outadj.size(); i++) {
            cout << *(outadj[i]);
            if (i < outadj.size()-1) 
              cout << "\t";
          }
          cout << "\n" << endl;
          graph.free_edge_vector(inadj);
          graph.free_edge_vector(outadj);
        }
      } else if (target == "shard_adj") {
        cout << "Trying to get shard content adjacent to: " << val << endl;
        graphlab::graph_shard_id_t shardid;
        vector<graphlab::graph_vid_t> vids;
        vector<string> strs;
        try {
          boost::split(strs, val, boost::is_any_of("\t ,"));
          if (strs.size() < 2) {
            cout << val  << " cannot be converted into shard id list<vids>" << endl;
            continue;
          }
          shardid = boost::lexical_cast<graphlab::graph_shard_id_t>(strs[0]);
          for (size_t i = 1; i < strs.size(); ++i) {
            vids.push_back(boost::lexical_cast<graphlab::graph_vid_t>(strs[i]));
          }
        } catch (boost::bad_lexical_cast &){
          cout << val  << " cannot be converted into shard id list<vids>" << endl;
          continue;
        }
        graphlab::graph_shard* shard = graph.get_shard_contents_adj_to(vids, shardid);
        if (shard)  {
          cout << *shard << std::endl;
          graph.free_shard(shard);
        } else {
          cout << "Fail getting shard content adjacent to " << val << std::endl;
        }
      } else {
        cout << "Unknown get target: " << target << " " << val << endl;
      }
    //------------------------ INGRESS commands ----------------------
    } else if (cmd == "add") { 
      if (target == "vertex") { // add vertex vid
        graphlab::graph_vid_t vid;
        try {
          vid = boost::lexical_cast<graphlab::graph_vid_t>(val);
        } catch (boost::bad_lexical_cast &) {
          cout << val << " cannot be converted into vertex id" << endl;
          continue;
        }
        cout << "Trying to add vertex " << vid << endl;
        graph.add_vertex_now(vid);
        cout << "Done" << endl;
      } else if (target == "edge") { // add edge srcid destid
        cout << "Trying to add edge " << val << endl;
        vector<string> strs;
        boost::split(strs, val, boost::is_any_of("\t ,"));
        if (strs.size() != 2) {
          cout << val << " cannot be converted to (src dest) pair" << endl;
          continue;
        }
        graphlab::graph_vid_t src, dest;
        try {
          src = boost::lexical_cast<graphlab::graph_vid_t>(strs[0]);
          dest = boost::lexical_cast<graphlab::graph_vid_t>(strs[1]);
        } catch (boost::bad_lexical_cast &) {
          cout << val << " cannot be converted to (src dest) pair" << endl;
          continue;
        }
        graph.add_edge_now(src, dest);
        cout << "Done" << endl;
      } else {
        cout << "Unknown add target: " << target << " " << val << endl;
      }
    } else if (cmd == "load") {
      cout << "Trying to load " << target << " format=" << val << endl;
      graph.load_format(target, val);
      cout << "Done" << endl;
    //------------------------ set commands ----------------------
    } else if (cmd == "set") { // set vertex vid fieldid data
      if (target == "vertex") {
        cout << "Trying to set vertex " << val << endl;
        vector<string> strs;
        boost::split(strs, val, boost::is_any_of("\t ,"));
        if (strs.size() != 3) {
          cout << val << " cannot be converted to (vid fieldpos data) pair" << endl;
          continue;
        }
        graphlab::graph_vid_t vid;
        size_t fieldpos;
        std::string data_str = strs[2];
        try {
          vid = boost::lexical_cast<graphlab::graph_vid_t>(strs[0]);
          fieldpos = boost::lexical_cast<size_t>(strs[1]);
        } catch (boost::bad_lexical_cast &) {
          cout << val << " cannot be converted to (vid fieldpos data) pair" << endl;
          continue;
        }
        graphlab::graph_vertex* v = graph.get_vertex(vid);
        v->data()->get_field(fieldpos)->set_val(data_str);
        v->write_changes();
        cout << "Done";
      } else {
        cout << "Unknown set target: " << target << " " << val << endl;
      }
    } else {
      cout << "Unknown command: " << cmd << " " << target << " " << val << endl;
    }
    cout << endl;
  }
}
