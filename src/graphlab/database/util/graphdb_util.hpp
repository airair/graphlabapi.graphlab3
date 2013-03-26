#ifndef GRAPHLAB_DATABASE_GRAPHDB_UTIL_HPP
#define GRAPHLAB_DATABASE_GRAPHDB_UTIL_HPP

#include <graphlab/logger/assertions.hpp>
#include <vector>

namespace graphlab {
  /// Returns the zip of two vectors. Assume the length of two input vectors are the same.
  template<typename T1, typename T2>
  std::vector<std::pair<T1, T2> > zip(const std::vector<T1> vec1, const std::vector<T2> vec2) {
    ASSERT_EQ(vec1.size(), vec2.size());
    std::vector<std::pair<T1, T2> >  ret;
    for (size_t i = 0; i < vec1.size(); i++) {
      ret.push_back( std::pair<T1, T2> (vec1[i], vec2[i]) );
    }
    return ret;
  }


}
#endif
