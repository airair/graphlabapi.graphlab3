/*
 * kvstore_base.cpp
 *
 *  Created on: Feb 4, 2013
 *      Author: svilen
 */

#ifndef GRAPHLAB_DATABASE_KVSTORE_BASE_HPP
#define GRAPHLAB_DATABASE_KVSTORE_BASE_HPP

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

  virtual boost::unique_future<std::pair<bool,value_type> > background_get(const key_type key) = 0;
  virtual boost::unique_future<std::vector<std::pair<bool, value_type> > > background_bulk_get(const std::vector<key_type> &keys) = 0;
  virtual boost::unique_future<std::vector<value_type> > background_range_get(const key_type key_lo, const key_type key_hi) = 0;

  virtual void remove_all() = 0;
//  virtual void remove(const key_type key) = 0;
//  virtual void background_remove(const key_type key) = 0;
//  virtual void range_remove(const key_type key_lo, const key_type key_hi) = 0;
//  virtual void background_range_remove(const key_type key_low, const key_type key_hi) = 0;
//  virtual void bulk_remove(const std::vector<key_type> &keys) = 0;
//  virtual void background_bulk_remove(const std::vector<key_type> &keys) = 0;
};

}

#endif
