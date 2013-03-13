#include <graphlab/database/server/graph_database_server.hpp>
#include <graphlab/database/query_messages.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>
#include "graph_database_test_util.hpp"
#include <vector>
using namespace std;

graphlab::QueryMessages query_messages;

void testVertexAdjacency(const string adjrep,
                         const std::vector<graphlab::graph_edge*>* inadj,
                         const std::vector<graphlab::graph_edge*>* outadj); 


void testVertexQuery(graphlab::graph_database_server* server) {
  bool success;

  graphlab::graph_database* db = server->get_database();

  for (size_t i = 0; i < db->num_vertices(); i++) {
    graphlab::graph_row row;
    std::string errormsg;
    // request each vertex row
    graphlab::graph_shard_id_t master = graphlab::graph_database_test_util::get_master(i, db->num_shards());
    int len;
    char* vdatareq =  query_messages.vertex_row_request(&len, i, master);
    std::string vdatarep = server->query(vdatareq, len);
    success = query_messages.parse_reply(vdatarep, row, errormsg);

    // compare to the vertex stored in the database
    graphlab::graph_vertex* v = db->get_vertex(i, master);
    ASSERT_EQ(success, (v!=NULL));

    if (success) {
      ASSERT_TRUE(row._own_data);
      const graphlab::graph_row* expected = v->immutable_data();
      graphlab::graph_database_test_util::compare_row(row, *expected);

      // compare adjacency structure
      bool prefetch_data = true;
      bool get_in = true;
      bool get_out = true;
      for (size_t j = 0; j < db->num_shards(); j++) {
          int msg_len;
          char* adjreq = query_messages.vertex_adj_request(&msg_len, i, j, get_in, get_out);
          std::string adjrep = server->query(adjreq, len);
          std::vector<graphlab::graph_edge*> _inadj;
          std::vector<graphlab::graph_edge*> _outadj;
          v->get_adj_list(j, prefetch_data, &_inadj, &_outadj);
          testVertexAdjacency(adjrep, &_inadj, &_outadj);
          db->free_edge_vector(_inadj);
          db->free_edge_vector(_outadj);
      }
    }
    db->free_vertex(v);
  }
}

void testVertexAdjacency(const string adjrep,
                         const std::vector<graphlab::graph_edge*>* inadj,
                         const std::vector<graphlab::graph_edge*>* outadj) {
    graphlab::iarchive iarc_adj(adjrep.c_str(), adjrep.length());
    std::string errormsg;
    size_t numin, numout;
    graphlab::graph_shard_id_t shardid;
    graphlab::graph_vid_t vid;
    bool success;
    iarc_adj >> success >> vid >> shardid >> numin >> numout;
    ASSERT_TRUE(success);
    ASSERT_EQ((numin==0), ((inadj==NULL)||(inadj->size()==0)));
    ASSERT_EQ((numout==0), ((outadj==NULL)||(outadj->size()==0)));
    for (size_t i = 0; i < numin; i++) {
      graphlab::graph_vid_t src;
      graphlab::graph_eid_t id;
      graphlab::graph_row data;
      iarc_adj >> src >> id >> data;

      graphlab::graph_edge* e = inadj->at(i);
      ASSERT_EQ(src, e->get_src());
      ASSERT_EQ(id, e->get_id());
      graphlab::graph_database_test_util::compare_row(data, *(e->immutable_data()));
    }
    for (size_t i = 0; i < numout; i++) {
      graphlab::graph_vid_t dest;
      graphlab::graph_eid_t id;
      graphlab::graph_row data;
      iarc_adj >> dest >> id >> data;

      graphlab::graph_edge* e = outadj->at(i);
      ASSERT_EQ(dest, e->get_dest());
      ASSERT_EQ(id, e->get_id());
      graphlab::graph_database_test_util::compare_row(data, *(e->immutable_data()));
    }
}

void testReadField(graphlab::graph_database_server* server) {
  bool success = false;
  std::string errormsg;
  vector<graphlab::graph_field> vfield;
  int len;
  char* vfieldreq = query_messages.vfield_request(&len);
  std::string vfieldrep = server->query(vfieldreq, len);
  success = query_messages.parse_reply(vfieldrep, vfield, errormsg);
  ASSERT_TRUE(success);
  ASSERT_EQ(vfield.size(), server->get_database()->get_vertex_fields().size());
  for (size_t i = 0; i < vfield.size(); i++) {
    ASSERT_TRUE(graphlab::graph_database_test_util::compare_graph_field(
            vfield[i], (server->get_database()->get_vertex_fields())[i]));
  }

  vector<graphlab::graph_field> efield;
  char* efieldreq = query_messages.efield_request(&len);
  std::string efieldrep = server->query(efieldreq, len);
  success = query_messages.parse_reply(efieldrep, efield, errormsg);
  ASSERT_TRUE(success);
  ASSERT_EQ(efield.size(), server->get_database()->get_edge_fields().size());
  for (size_t i = 0; i < efield.size(); i++) {
    ASSERT_TRUE(graphlab::graph_database_test_util::compare_graph_field(
            efield[i], (server->get_database()->get_edge_fields())[i]));
  }
}

int main(int argc, char** argv)
{
  size_t nverts = 100;
  size_t nedges = 2000;
  size_t nshards = 5;

  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;

  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  edgefields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));

  graphlab::graph_database* db =
      graphlab::graph_database_test_util::createDatabase(nverts, nedges, nshards,
                                                         vertexfields, edgefields);
  graphlab::graph_database_server server(db);
  testReadField(&server);
  testVertexQuery(&server);
  delete db;
  return 0;
}
