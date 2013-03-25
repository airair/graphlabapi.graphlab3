#ifndef GRAPHLAB_DATABASE_QUERY_MESSAGE_HPP
#define GRAPHLAB_DATABASE_QUERY_MESSAGE_HPP
#include<graphlab/serialization/iarchive.hpp>
#include<graphlab/serialization/oarchive.hpp>
namespace graphlab {
  /**
   * This class defines the query message protocol from 
   * <code>graphdb_client</code> to <code>graphdb_server</code>
   * It supports serialization and deserialized.
   *
   * Send query example: 
   *  QueryMessage qm(cmd, objective);
   *  qm << arg0 << arg1 << arg2... ;
   *  int len = qm.length();
   *  char* msg = qm.message();
   *  send(msg, len); 
   *
   * Receive query example:
   *  QueryMessage qm(msg, len);
   *  QueryMessage::header = qm.get_header();
   *  switch(header.cmd) {
   *    CASE xxx: 
   *      T1 arg1; T2 arg2;
   *      if (header.obj == QueryMessage::GET) {
   *        qm >> arg1 >> arg2;
   *        ....
   *      } 
   *      break;
   *  }
   */
  class QueryMessage {
   public:
     enum qm_cmd_type{ 
       GET, SET, ADD,
       // batch commands
       BGET, BSET, BADD };

     enum qm_obj_type{ 
       VERTEX, EDGE, VERTEXADJ, VMIRROR, SHARD, 
       NVERTS, NEDGES, VFIELD, EFIELD, UNDEFINED
     };

     static const char* qm_cmd_type_str[6]; 

     static const char* qm_obj_type_str[11];

     struct header {
       qm_cmd_type cmd;
       qm_obj_type obj;

       header() { }
       header(qm_cmd_type cmd, qm_obj_type obj) : cmd(cmd), obj(obj) { }

       friend std::ostream& operator<<(std::ostream &strm, const header& h) {
          strm << qm_cmd_type_str[h.cmd] << " " << qm_obj_type_str[h.obj];
          return strm;
       }
     };

   public:
     // --------------------- Serialization  -----------------------
     /// Creates a query message with given header.
     QueryMessage(header h);

     /// Creates a query message with given command and object.
     QueryMessage(qm_cmd_type cmd, qm_obj_type obj);

    /// Serialize the argument into the message;
    template<typename T>
    QueryMessage& operator<<(const T& value) {
      oarc << value; 
      return *this;
    }

    /// Returns the length of the serialized message;
    inline int length() { return oarc.off; }

    /// Returns the pointer to the serialized message;
    inline char* message() { return oarc.buf; }

    // ----------------------- Deserialization -----------------------
     /// Creates a query message from given string, parse the command and object part.
     QueryMessage(char* msg, size_t len);

     ~QueryMessage();

     inline header get_header() { return h; }

     /// Deserialize the argument from the message;
     template<typename T>
     QueryMessage& operator>>(T& value) {
       *iarc >> value; 
       return *this;
     }
   private:
    header h;
    oarchive oarc;
    iarchive* iarc;
  }; // end of class
} // end of namespace
#endif
