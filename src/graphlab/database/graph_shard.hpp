#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_HPP 
#include <utility>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_shard_impl.hpp>
#include <boost/functional/hash.hpp>

// forward declaration
class graph_database;
class graph_database_sharedmem;

namespace graphlab {

/**
 * \ingroup group_graph_database
 * A structure which provides access to a low level representation of the 
 * contents of a graph shard. Only the data of the shard is mutable.
 * The shard structure (cannot at the moment) be changed.
 *
 * \note It is actually somewhat annoying that most of the stuff here are
 * raw C style arrays rather than std::vector. Most std::vector
 * operations rely on the copy constructor. However, it is highly desirable that
 * graph_row not be copyable. C++11 will resolve some of this through the 
 * emplace functions, but it produces really odd semantics in that the struct
 * provides a std::vector class where many of the vector functions are unsafe
 * to use (or will produce compilation errors).
 *
 * \note The contents are split up into a separate struct with all fields
 * public to avoid friend-ship issues with graph_shard, allowing the 
 * graph_shard_impl to be used internally in the graph database implementations,
 * only converting it to a graph_shard at the last moment.
 * 
 * \note The graph_shard is a friend of graph_database and thus a friend
 * of all descendents of the graph_database
 *
 * \note Only information of the master vertices are stored within the shard.
 *
 * \note Special case: A shard fetched through <cod>graph_database::get_shard_contents_adj_to</code> does not have vertex information.
 */
class graph_shard {
 private:
   graph_shard_impl shard_impl;

 public:
   graph_shard() { }

   graph_shard(const graph_shard_impl& shard_impl): shard_impl(shard_impl) { }
   
   ~graph_shard() {
      for (size_t i = 0; i < num_vertices(); ++i) {
         delete(shard_impl.vertex_data[i]);
      }
      for (size_t i = 0; i < num_edges(); ++i) {
         delete(shard_impl.edge_data[i]);
      }
   }

   /**
    * Returns the id of this shard.
    * Id is -1 if this is a derived shard.
    */
   inline graph_shard_id_t id() {
     return shard_impl.shard_id;
   }

  /**
   * Returns the number of vertices in this shard
   */ 
  inline size_t num_vertices() { return shard_impl.num_vertices; }

  /**
   * Returns the number of edges in this shard
   */ 
  inline size_t num_edges() { return shard_impl.num_edges; }

  /**
   * Returns the ID of the vertex in the i'th position in this shard.
   * i must range from 0 to num_vertices() - 1 inclusive.
   */ 
  inline graph_vid_t vertex(size_t i) { return shard_impl.vertex[i]; }

  /**
   * Returns the data of the vertex in the i'th position in this shard.
   * vertex_data(i) corresponds to the data on the vertex with ID vertex(i)
   * i must range from 0 to num_vertices() - 1 inclusive.
   */
  inline graph_row* vertex_data(size_t i) { return shard_impl.vertex_data[i]; }
  
  /**
   * Returns the number of out edges of the vertex in the i'th position in this
   * shard. This counts the total number of out edges of this vertex in the graph.
   * num_out_edges(i) is the number of out edges of the vertex with ID vertex(i)
   * i must range from 0 to num_vertices() - 1 inclusive.
   * \note Deprecated because shard only store master vertices.
   */
  // inline size_t num_out_edges(size_t i) { return shard_impl.num_out_edges[i]; }
  
  /**
   * Returns the number of in edges of the vertex in the i'th position in this
   * shard. This counts the total number of in edges of this vertex in the graph.
   * num_in_edges(i) is the number of in edges of the vertex with ID vertex(i)
   * i must range from 0 to num_vertices() - 1 inclusive.
   * \note Deprecated because shard only store master vertices.
   */
  // inline size_t num_in_edges(size_t i) { return shard_impl.num_in_edges[i]; }

  /**
   * Returns edge in the j'th position in this shard.
   * The edge is a pair of (src vertex ID, dest vertex ID).
   * j must range from 0 to num_edges() - 1 inclusive.
   */
  inline std::pair<graph_vid_t, graph_vid_t> 
      edge(size_t j) { return shard_impl.edge[j]; }

  /**
   * Returns the data of the edge in the j'th position in this shard.
   * edge_data(i) corresponds to the data on the edge edge(i)
   * i must range from 0 to num_edges() - 1 inclusive.
   */
  inline graph_row* edge_data(size_t i) { return shard_impl.edge_data[i]; }


// ----------- Modification API -----------------
  /**
   * Clear the content of this shard. Remove all vertex and edge data.
   */
  inline void clear() {
      // for (size_t i = 0; i < num_vertices(); ++i) {
      //    free(shard_impl.vertex_data[i]);
      // }
      // for (size_t i = 0; i < num_edges(); ++i) {
      //    free(shard_impl.edge_data[i]);
      // }
      shard_impl.vertex.clear();
      shard_impl.edge.clear();
      shard_impl.edgeid.clear();
      shard_impl.vertex_data.clear();
      shard_impl.edge_data.clear();
      // shard_impl.num_out_edges.clear();
      // shard_impl.num_in_edges.clear();
      shard_impl.num_vertices = shard_impl.num_edges = 0;
  }


 private:

  // copy constructor deleted. It is not safe to copy this object.
  graph_shard(const graph_shard&) { }

  // assignment operator deleted. It is not safe to copy this object.
  graph_shard& operator=(const graph_shard&) { return *this; }

  friend class graph_database;
  friend class graph_database_sharedmem;
};

} // namespace graphlab 
#endif
