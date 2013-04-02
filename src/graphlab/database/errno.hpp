#ifndef GRAPHLAB_DATABASE_ERRORNO_HPP
#define GRAPHLAB_DATABASE_ERRORNO_HPP
#include <errno.h>
#include <boost/lexical_cast.hpp>
#define ESRVUNREACH 1000  /* Server unreachable */
#define EINVID 1001  /* Invalid (vertex/edge/shard/field) id */
#define EINVTYPE 1002 /* Invalid datatype */
#define EDUP 1003 /* Duplicate objects (vertex already exists) */
#define EINVHEAD 1004 /* Invalid query header */
#define EINVCMD 1005 /* Invalid command */
namespace graphlab {
  inline std::string glstrerr (int errorno) {
    switch (errorno) {
     case ESRVUNREACH: return "Server unreachable";
     case EINVID: return "Invalid (verte/edge/shard/field) id";
     case EINVTYPE: return "Invalid datatype";
     case EDUP: return "Duplicate objects (vertex/field already exists)";
     case EINVHEAD: return "Invalid query header";
     case EINVCMD: return "Invalid command";
     default: return strerror(errorno);
    }
  }
}// namespace
#endif
