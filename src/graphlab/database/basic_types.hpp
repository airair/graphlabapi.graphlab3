#ifndef GRAPHLAB_DATABASE_BASIC_TYPES_HPP
#define GRAPHLAB_DATABASE_BASIC_TYPES_HPP
#include <string>
#include <vector>
#include <stdint.h>
#include <graphlab/logger/assertions.hpp>
#include <boost/algorithm/string.hpp>

namespace graphlab {

/// \ingroup group_graph_database
/// identifies a field id
typedef uint16_t graph_field_id_t;
/// identifies a vertex id
// typedef __uint128_t graph_vid_t;
typedef uint64_t graph_vid_t;
/// identifies an edge id
typedef uint64_t graph_eid_t;
/// identifies an edge local index within the shard 
typedef uint32_t graph_leid_t;
/// IDs used for shards
typedef uint16_t graph_shard_id_t;
union eid_union {
  graph_eid_t eid;
  struct {
    graph_shard_id_t shard_id : 16;
    graph_leid_t local_eid : 32; 
  } split;
};

inline std::pair<graph_shard_id_t, graph_leid_t> split_eid(graph_eid_t eid) {
  eid_union u;
  u.eid = eid;
  std::pair<graph_shard_id_t, graph_leid_t> pair;
  pair.first = u.split.shard_id;
  pair.second = u.split.local_eid;
  return pair;
}

inline graph_eid_t make_eid(graph_shard_id_t shardid, graph_leid_t leid) {
  eid_union u;
  u.split.shard_id = shardid;
  u.split.local_eid = leid;
  return u.eid;
}

/// \ingroup group_graph_database
/// integer field data type 
typedef int64_t graph_int_t;

/// \ingroup group_graph_database
/// double field data type
typedef double graph_double_t;

/// \ingroup group_graph_database
/// string field data type
typedef std::string graph_string_t;

/// \ingroup group_graph_database
/// blob field data type
typedef std::string graph_blob_t;

/*
typedef Eigen::VectorXd graphlab_vecd_t;
typedef Eigen::VectorXcd graphlab_veccd_t;
typedef Eigen::VectorXi graphlab_veci_t;

typedef Eigen::MatrixXd graphlab_matd_t;
typedef Eigen::MatrixXcd graphlab_matcd_t;
typedef Eigen::MatrixXi graphlab_mati_t;
*/

typedef std::vector<double> graph_d_vector_t;

/// \ingroup group_graph_database
// Possible data types for fields
enum graph_datatypes_enum {
	VID_TYPE,
	INT_TYPE,
	DOUBLE_TYPE,
	STRING_TYPE,
	BLOB_TYPE,
  DOUBLE_VEC_TYPE,
  UNKNOWN_TYPE
};

/// \ingroup group_graph_database
inline bool is_scalar_graph_datatype(graph_datatypes_enum e) {
  switch(e) {
   case VID_TYPE: 
   case INT_TYPE: 
   case DOUBLE_TYPE:
      return true;
   default:
      return false;
  }
}

static const char* graph_datatypes_string[] = {
  "VID",
  "INT",
  "DOUBLE",
  "STRING",
  "BLOB",
  "DOUBLE_VEC"
};

inline graph_datatypes_enum string_to_type(std::string typestr) {
  boost::algorithm::to_lower(typestr); 
  if (typestr == "int") {
    return INT_TYPE;
  } else if (typestr == "double") {
    return DOUBLE_TYPE;
  } else if (typestr == "string") {
    return STRING_TYPE;
  } else if (typestr == "blob") {
    return BLOB_TYPE;
  } else if (typestr == "double_vec") {
    return DOUBLE_VEC_TYPE;
  } else if (typestr == "vid") {
    return VID_TYPE;
  } else {
    return UNKNOWN_TYPE;
  } 
}
} // namespace graphlab

#endif
