#ifndef GRAPHLAB_DATABASE_GRAPH_SHARD_HPP
#define GRAPHLAB_DATABASE_GRAPH_SHARD_HPP 
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_shard_impl.hpp>
#include <graphlab/logger/assertions.hpp>
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
   inline graph_shard() { }
   
   inline graph_shard(graph_shard_id_t shardid) {
     shard_impl.shard_id = shardid;
   }
   
   inline ~graph_shard() {}

   void clear() { shard_impl.clear(); }

   /**
    * Returns the id of this shard.
    * Id is -1 if this is a derived shard.
    */
   inline graph_shard_id_t id() const { return shard_impl.shard_id; }

  /**
   * Returns the number of vertices in this shard
   */ 
  inline size_t num_vertices() const { return shard_impl.vertex.size(); }

  /**
   * Returns the number of edges in this shard
   */ 
  inline size_t num_edges() const { return shard_impl.edge.size(); }

  /**
   * Returns the ID of the vertex in the i'th position in this shard.
   * i must range from 0 to num_vertices() - 1 inclusive.
   */ 
  inline graph_vid_t vertex(size_t i) const { return shard_impl.vertex[i]; }

  /**
   * Returns the data of the vertex in the i'th position in this shard.
   * vertex_data(i) corresponds to the data on the vertex with ID vertex(i)
   * i must range from 0 to num_vertices() - 1 inclusive.
   */
  inline graph_row* vertex_data(size_t i) { 
    ASSERT_LT(i, num_vertices()); 
    return &(shard_impl.vertex_data[0])+i;
  }

   /**
    * Return true if the vertex with vid is owned by this shard.
    */
   inline bool has_vertex(const graph_vid_t& vid) const {
     return shard_impl.vertex_index.has_vertex(vid);
   }

  /**
    * Return the mirror information of the vertex in the i'th position.
    */
   inline const std::vector<graph_shard_id_t> mirrors(size_t i) const {
     const boost::unordered_set<graph_shard_id_t> mirrorset = shard_impl.vertex_mirrors[i];
     std::vector<graph_shard_id_t> ret(mirrorset.begin(), mirrorset.end());
     return ret;
   }

  /**
    * Return true if the vertex with vid is owned by this shard.
    */
   inline const std::vector<graph_shard_id_t> mirrors_by_id(const graph_vid_t& vid) const {
     size_t pos = shard_impl.vertex_index.get_index(vid);
     return mirrors(pos);
   }

   /**
    * Returns the data of vertex with vid. Return NULL if there is no vertex 
    * data associated with vid in this shard.
    */
   inline graph_row* vertex_data_by_id (const graph_vid_t& vid) {
     if (has_vertex(vid)) {
       return vertex_data(shard_impl.vertex_index.get_index(vid));
     } else {
       return NULL;
     }
   }

   /**
   * Returns edge in the j'th position in this shard.
   * The edge is a pair of (src vertex ID, dest vertex ID).
   * j must range from 0 to num_edges() - 1 inclusive.
   */
  inline std::pair<graph_vid_t, graph_vid_t> edge(size_t j) const { return shard_impl.edge[j]; }

  /**
   * Returns the data of the edge in the j'th position in this shard.
   * edge_data(i) corresponds to the data on the edge edge(i)
   * i must range from 0 to num_edges() - 1 inclusive.
   */
  inline graph_row* edge_data(size_t i) {
    return &(shard_impl.edge_data[0]) + i; 
  }

  /**
   * Fill the adjacency structure of given vertex withvid.
   */
  inline void vertex_adj_ids (std::vector<graph_leid_t>& outids, 
                          graph_vid_t vid, bool is_in_edges) const { 
    shard_impl.edge_index.get_vertex_adj(outids, vid, is_in_edges);
  }

  /**
   * Returns the adjacency data of given vertex withvid.
   */
  inline void vertex_adj (std::vector<graph_vid_t>& out, 
                          graph_vid_t vid, bool is_in_edges) const { 
    std::vector<graph_leid_t> ids;
    shard_impl.edge_index.get_vertex_adj(ids, is_in_edges, vid);
    if (is_in_edges) {
      for (size_t i = 0; i < ids.size(); i++) {
        out.push_back(shard_impl.edge[ids[i]].first);
      }
    } else {
      for (size_t i = 0; i < ids.size(); i++) {
        out.push_back(shard_impl.edge[ids[i]].second);
      }
    }
  }

  // ----------- Modification API -----------------
  /**
   * Insert a (vid, row) into the shard. Return the position of the vertex in the shard.
   * For optimization, take over the row pointer.
   */
  size_t add_vertex(graph_vid_t vid, const graph_row& row) {
    return shard_impl.add_vertex(vid, row);
  };

  /**
   * Insert a (vid, mirror) record into the shard. Shard must be the master of the vertex.
   */
  void add_vertex_mirror(graph_vid_t v, graph_shard_id_t mirror_id) {
    shard_impl.add_vertex_mirror(v, mirror_id);
  };

  /**
   * Insert a (source, target, row) into the shard. Return the position of the edge in the shard.
   * For optimization purpose, the data ownership of row is transfered.
   */
  size_t add_edge(graph_vid_t source, graph_vid_t target, const graph_row& row) {
    return shard_impl.add_edge(source, target, row);
  }

  // ----------- Serialization API ----------
  inline void save(oarchive& oarc) const {
    oarc << shard_impl;
  }

  inline void load(iarchive& iarc) {
    iarc >> shard_impl;
  }


  // Print the shard summary
  friend std::ostream& operator<<(std::ostream &strm, const graph_shard& shard) {
    return strm << "Shard " << shard.id() << "\n"
                << "num vertices: " << shard.num_vertices() << "\n"
                << "num edges: " << shard.num_edges() << "\n";
  }

  
 private:
  // copy constructor deleted. It is not safe to copy this object.
  graph_shard(const graph_shard&) { }

  // assignment operator deleted. It is not safe to copy this object.
  graph_shard& operator=(const graph_shard&) { return *this; }
};
} // namespace graphlab 
#endif
 
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


