#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
using namespace std;

graphlab::graph_database* createDatabase() {
  vector<graphlab::graph_field> vertexfields;
  vector<graphlab::graph_field> edgefields;
  vertexfields.push_back(graphlab::graph_field("url", graphlab::STRING_TYPE));
  vertexfields.push_back(graphlab::graph_field("pagerank", graphlab::DOUBLE_TYPE));
  edgefields.push_back(graphlab::graph_field("weight", graphlab::DOUBLE_TYPE));
  size_t nshards = 4;

  graphlab::graph_database_sharedmem* db = new graphlab::graph_database_sharedmem (vertexfields, edgefields, nshards);

  size_t nverts = 100;
  size_t nedges = 2000;
  boost::hash<size_t> hash; 
  // Creates a random graph
  for (size_t i = 0; i < nedges; i++) {
    size_t source = hash(i) % nverts;
    size_t target = hash(-i) % nverts;
    db->add_edge(source, target);
  }
  return db;
}

void testVertexAdjacency(const string adjrep,
                         const std::vector<graphlab::graph_edge*>* inadj,
                         const std::vector<graphlab::graph_edge*>* outadj); 


bool compare_value(const graphlab::graph_value& lhs, const graphlab::graph_value& rhs) {
  if ((lhs._type != rhs._type)
      || (lhs._null_value != rhs._null_value)
      || (lhs._len != rhs._len)) {
    return false;
  }
  bool eq = false;
  switch (lhs._type) {
   case graphlab::INT_TYPE:
     eq = (lhs._data.int_value == rhs._data.int_value); break;
   case graphlab::DOUBLE_TYPE:
     eq = (lhs._data.double_value == rhs._data.double_value); break;
   case graphlab::VID_TYPE:
     eq = (lhs._data.vid_value == rhs._data.vid_value); break;
   case graphlab::BLOB_TYPE:
   case graphlab::STRING_TYPE:
     eq = ((memcmp(lhs._data.bytes, rhs._data.bytes, lhs._len)) == 0); break;
  }
  return eq;
}

bool compare_row(graphlab::graph_row& lhs, graphlab::graph_row& rhs) {
  if ((lhs.is_vertex() != rhs.is_vertex()) || (lhs.num_fields() != rhs.num_fields()))
    return false;

  bool eq = true;
  for (size_t j = 0; eq && j < lhs.num_fields(); j++) {
    eq = compare_value(*(lhs.get_field(j)), *(rhs.get_field(j)));
  }
  return eq;
}


std::string create_vertex_row_request(graphlab::graph_vid_t vid) {
  graphlab::oarchive oarc;
  oarc << std::string("vertex_data_row") << vid;
  std::string s(oarc.buf, oarc.off);
  free(oarc.buf);
  return s;
}

std::string create_vertex_adj_request(graphlab::graph_vid_t vid,
                                      graphlab::graph_shard_id_t shardid,
                                      bool getin,
                                      bool getout,
                                      bool prefetch_data) {
  graphlab::oarchive oarc;
  oarc << std::string("vertex_adj") << vid << shardid << getin << getout << prefetch_data;
  std::string s(oarc.buf, oarc.off);
  free(oarc.buf);
  return s;
}

std::string create_vfield_request() {
  graphlab::oarchive oarc;
  oarc << std::string("vertex_fields_meta");
  std::string s(oarc.buf, oarc.off);
  free(oarc.buf);
  return s;
}

std::string create_efield_request() {
  graphlab::oarchive oarc;
  oarc << std::string("edge_fields_meta");
  std::string s(oarc.buf, oarc.off);
  free(oarc.buf);
  return s;
}


void testReadVertexData(graphlab::graph_database_server* server) {
  bool success;
  graphlab::graph_database* db = server->get_database();

  for (size_t i = 0; i < db->num_vertices(); i++) {
    graphlab::graph_row row;

    std::string vdatareq =  create_vertex_row_request(i);
    std::string vdatarep = server->query(vdatareq.c_str(), vdatareq.length());

    graphlab::iarchive iarc_vdata(vdatarep.c_str(), vdatarep.length());
    iarc_vdata >> success  >> row;

    graphlab::graph_vertex* v = db->get_vertex(i);
    ASSERT_EQ(success, (v!=NULL));
    if (success) {
      ASSERT_TRUE(row._own_data);
      graphlab::graph_row* expected = v->data();
      ASSERT_EQ(row.num_fields(), expected->num_fields());
      compare_row(row, *expected);

      bool prefetch_data = true;
      bool get_in = true;
      bool get_out = true;
      for (size_t j = 0; j < db->num_shards(); j++) {
          std::string adjreq = create_vertex_adj_request(i, j, get_in, get_out, prefetch_data);
          std::string adjrep = server->query(adjreq.c_str(), adjreq.length());

          std::vector<graphlab::graph_edge*> _inadj;
          std::vector<graphlab::graph_edge*> _outadj;
          v->get_adj_list(j, prefetch_data, &_inadj, &_outadj);

          testVertexAdjacency(adjrep, &_inadj, &_outadj);

          db->free_edge_vector(&_inadj);
          db->free_edge_vector(&_outadj);
      }
    }
    db->free_vertex(v);
  }
}

void testVertexAdjacency(const string adjrep,
                         const std::vector<graphlab::graph_edge*>* inadj,
                         const std::vector<graphlab::graph_edge*>* outadj
                         ) {
    graphlab::iarchive iarc_adj(adjrep.c_str(), adjrep.length());
    size_t numin, numout;
    bool prefetch_data;
    bool success;
    iarc_adj >> success >> numin >> numout >> prefetch_data;
    ASSERT_TRUE(success);
    ASSERT_EQ((numin==0), ((inadj==NULL)||(inadj->size()==0)));
    ASSERT_EQ((numout==0), ((outadj==NULL)||(outadj->size()==0)));
    for (size_t i = 0; i < numin; i++) {
      graphlab::graph_vid_t src;
      graphlab::graph_eid_t id;
      graphlab::graph_row data;
      iarc_adj >> src >> id;
      if (prefetch_data)
        iarc_adj >> data;

      graphlab::graph_edge* e = inadj->at(i);
      ASSERT_EQ(src, e->get_src());
      ASSERT_EQ(id, e->get_id());
      if (prefetch_data) {
        compare_row(data, *(e->data()));
      }
    }
    for (size_t i = 0; i < numout; i++) {
      graphlab::graph_vid_t dest;
      graphlab::graph_eid_t id;
      graphlab::graph_row data;
      iarc_adj >> dest >> id;
      if (prefetch_data)
        iarc_adj >> data;

      graphlab::graph_edge* e = outadj->at(i);
      ASSERT_EQ(dest, e->get_dest());
      ASSERT_EQ(id, e->get_id());
      if (prefetch_data) {
        compare_row(data, *(e->data()));
      }
    }
}

void testReadField(graphlab::graph_database_server* server) {
  bool success = false;
  vector<graphlab::graph_field> vfield;
  std::string vfieldreq = create_vfield_request();
  std::string vfieldrep = server->query(vfieldreq.c_str(), vfieldreq.length());
  graphlab::iarchive iarc_vfield(vfieldrep.c_str(), vfieldrep.length());
  iarc_vfield >> success;
  ASSERT_TRUE(success);
  iarc_vfield >> vfield;
  ASSERT_EQ(vfield.size(), server->get_database()->get_vertex_fields().size());

  vector<graphlab::graph_field> efield;
  std::string efieldreq = create_efield_request();
  std::string efieldrep = server->query(efieldreq.c_str(), efieldreq.length());
  graphlab::iarchive iarc_efield(efieldrep.c_str(), efieldrep.length());
  iarc_efield >> success  >> efield;
  ASSERT_TRUE(success);
  ASSERT_EQ(efield.size(), server->get_database()->get_edge_fields().size());
}

int main(int argc, char** argv)
{
  graphlab::graph_database* db = createDatabase();
  graphlab::graph_database_server server(db);
  testReadField(&server);
  testReadVertexData(&server);
  delete db;
  return 0;
}
