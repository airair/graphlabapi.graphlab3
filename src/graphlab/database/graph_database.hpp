#ifndef GRAPHLAB_DATABASE_GRAPH_DATABASE_HPP
#define GRAPHLAB_DATABASE_GRAPH_DATABASE_HPP
#include <vector>
#include <graphlab/database/basic_types.hpp>
#include <graphlab/database/graph_field.hpp>
#include <graphlab/database/graph_vertex.hpp>
#include <graphlab/database/graph_edge.hpp>
#include <graphlab/database/graph_row.hpp>
#include <graphlab/database/graph_shard.hpp>

#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
namespace graphlab {
/**
 * \ingroup group_graph_database
 * An abstract interface for a graph database implementation 
 */
class graph_database {
 public:
  // Store the query result of a vertex adjacency
  struct vertex_adj_descriptor {
     std::vector<graph_vid_t> neighbor_ids;
     std::vector<graph_eid_t> eids;

     vertex_adj_descriptor& operator+=(const vertex_adj_descriptor& other) {
       neighbor_ids.insert(neighbor_ids.end(), other.neighbor_ids.begin(), other.neighbor_ids.end());
       eids.insert(eids.end(), other.eids.begin(), other.eids.end());
       return *this;
     }

     void save (oarchive& oarc) const {
       oarc << neighbor_ids << eids;
     }
     void load (iarchive& iarc)  {
       iarc >> neighbor_ids >> eids;
     }
   };

   struct vertex_insert_descriptor {
     graph_vid_t vid;
     graph_row data;
     void save (oarchive& oarc) const {
       oarc << vid << data;
     }
     void load (iarchive& iarc)  {
       iarc >> vid >> data;
     }
   };

   struct edge_insert_descriptor {
     graph_vid_t src, dest;
     graph_row data;
     void save (oarchive& oarc) const {
       oarc << src << dest << data;
     }
     void load (iarchive& iarc)  {
       iarc >> src >> dest >> data;
     }
   };

 public:
  virtual ~graph_database() { }

  /// Returns the number of vertices in the graph.
  /// This may be slow.
  virtual uint64_t num_vertices() = 0;
  
  /// Returns the number of edges in the graph.
  /// This may be slow.
  virtual uint64_t num_edges() = 0;

  /// Returns the field metadata for the vertices in the graph
  virtual const std::vector<graph_field> get_vertex_fields() = 0;

  /// Returns the field metadata for the edges in the graph
  virtual const std::vector<graph_field> get_edge_fields() = 0;

  // --------------------- Schema Modification API ----------------------
  /// Add a field to the vertex data schema
  virtual int add_vertex_field(const graph_field& field) = 0;

  /// Add a field to the edge data schema
  virtual int add_edge_field(const graph_field& field) = 0;

  // --------------------- Structure Modification API ----------------------
  virtual int add_vertex(graph_vid_t vid, const graph_row& data) = 0;

  virtual int add_edge(graph_vid_t source, graph_vid_t target, const graph_row& data) = 0;

  // --------------------- Batch Structure Modification API ----------------------
  virtual bool add_vertices(const std::vector<vertex_insert_descriptor>& vertices,
                            std::vector<int>& errorcodes) = 0;

  virtual bool add_edges(const std::vector<edge_insert_descriptor>& edges,
                         std::vector<int>& errorcodes) = 0;

  // --------------------- Single Query API -----------------------------------------
  // Read API
  virtual int get_vertex(graph_vid_t vid, graph_row& out) = 0;
  virtual int get_edge(graph_eid_t eid, graph_row& out)  = 0;
  virtual int get_vertex_adj(graph_vid_t vid, bool in_edges, vertex_adj_descriptor& out) = 0;

  // Write API
  virtual int set_vertex(graph_vid_t vid, const graph_row& data) = 0;
  virtual int set_edge(graph_eid_t eid, const graph_row& data) = 0;


  // --------------------- Batch Query API -----------------------------------------
  virtual bool get_vertices (const std::vector<graph_vid_t>& vids,
                             std::vector<graph_row>& out, 
                             std::vector<int>& errorcodes) = 0;
  virtual bool get_edges (const std::vector<graph_eid_t>& eids,
                          std::vector<graph_row>& out, 
                          std::vector<int>& errorcodes) = 0;

  virtual bool set_vertices (const std::vector< std::pair<graph_vid_t, graph_row> >& pairs, 
                             std::vector<int>& errorcodes) = 0;
  virtual bool set_edges (const std::vector< std::pair<graph_eid_t, graph_row> >& pairs,
                          std::vector<int>& errorcodes) = 0;

  // ---------------------- Base Utility Function -------------------------
  /**
   * Returns the index of the vertex column with the given field name. 
   *
   * \note For database implementors: A default implementation using a linear 
   * search of \ref get_edge_fields() is provided.
   */
  virtual int find_vertex_field(const char* fieldname) {
    const std::vector<graph_field>& vfields = get_vertex_fields();
    for (size_t i = 0;i < vfields.size(); ++i) {
      if (vfields[i].name.compare(fieldname) == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Returns the index of the edge column with the given field name. 
   *
   * \note For database implementors: A default implementation using a linear 
   * search of \ref get_edge_fields() is provided.
   */
  virtual int find_edge_field(const char* fieldname) {
    const std::vector<graph_field>& efields = get_edge_fields();
    for (size_t i = 0;i < efields.size(); ++i) {
      if (efields[i].name.compare(fieldname) == 0) {
        return i;
      }
    }
    return -1;
  }
  };
} // namespace graphlab

#endif

