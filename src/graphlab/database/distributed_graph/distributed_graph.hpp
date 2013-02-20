#ifndef GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_HPP
#define GRAPHLAB_DATABASE_DISTRIBUTED_GRAPH_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/logger/logger.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_shard.hpp>
#include <graphlab/database/graph_database.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>

namespace graphlab {
  class distributed_graph {

   public:
    distributed_graph() { }
    virtual ~distributed_graph() { }
  };
}
#endif
