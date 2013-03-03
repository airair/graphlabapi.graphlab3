#ifndef GRAPHLAB_DATABASE_BASIC_TYPES_HPP
#define GRAPHLAB_DATABASE_BASIC_TYPES_HPP
#include <string>
#include <vector>
#include <stdint.h>

namespace graphlab {

/// \ingroup group_graph_database
/// identifies a vertex id
// typedef __uint128_t graph_vid_t;
typedef uint64_t graph_vid_t;
typedef uint64_t graph_eid_t;

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
/// Possible data types for fields
enum graph_datatypes_enum {
	VID_TYPE,
	INT_TYPE,
	DOUBLE_TYPE,
	STRING_TYPE,
	BLOB_TYPE,
  DOUBLE_VEC_TYPE
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


/// \ingroup group_graph_database
/// IDs used for shards
typedef uint32_t graph_shard_id_t;

} // namespace graphlab

#endif
