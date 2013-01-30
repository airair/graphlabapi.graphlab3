#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_SHAREDMEM_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_vertex_index.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {
/**
 * \ingroup group_graph_database_sharedmem
 * An shared memory implementation of a graph database
 */
class graph_database_sharedmem : public graph_database {
 public:
   graph_database_sharedmem() { }
};
} // namespace graphlab
#include <graphlab/macros_undef.hpp>
#endif

