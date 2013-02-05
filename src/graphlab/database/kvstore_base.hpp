/*
 * kvstore_base.cpp
 *
 *  Created on: Feb 4, 2013
 *      Author: svilen
 */

#ifndef GRAPHLAB_DATABASE_KVSTORE_BASE.HPP
#define GRAPHLAB_DATABASE_KVSTORE_BASE.HPP

#include <stdint.h>
#include <string>
#include <vector>

#include <boost/thread/future.hpp>

namespace graphlab {

typedef uint64_t key_type;
typedef std::string value_type;

class kvstore_base {
public:
  virtual void set(const key_type key, const value_type &value) = 0;
  virtual void background_set(const key_type key, const value_type &value) = 0;

  virtual bool get(const key_type key, value_type &value) = 0;
  virtual std::vector<std::pair<bool, value_type> > bulk_get(const std::vector<key_type> &keys) = 0;
  virtual std::vector<value_type> range_get(const key_type key_lo, const key_type key_hi) = 0;

  virtual boost::unique_future<std::pair<bool, value_type>> background_get(const key_type key) = 0;
  virtual boost::unique_future<std::vector<std::pair<bool, value_type> > > background_bulk_get(const std::vector<key_type> &keys) = 0;
  virtual boost::unique_future<std::vector<value_type> > background_range_get(const key_type key_lo, const key_type key_hi) = 0;
};

}

#endif
