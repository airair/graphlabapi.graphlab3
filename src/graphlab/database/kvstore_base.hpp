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
#include <boost/foreach.hpp>
#include <boost/thread.hpp>
#include <boost/move/move.hpp>

namespace graphlab {

typedef uint64_t key_type;
typedef std::string value_type;

class kvstore_base {
public:
  virtual void set(const key_type key, const value_type &value) = 0;
  virtual void background_set(const key_type key, const value_type &value) {
    boost::thread t(&kvstore_base::set, this, key, value);
    t.detach();
  }

  virtual bool get(const key_type key, value_type &value) = 0;

  virtual std::vector<std::pair<bool, value_type> > bulk_get(const std::vector<key_type> &keys) {
    std::vector<std::pair<bool, value_type> > result;
    bool r;
    value_type v;
    BOOST_FOREACH(key_type key, keys) {
      r = get(key, v);
      result.push_back(std::pair<bool, value_type>(r, v));
    }
    return result;
  }

  virtual std::vector<value_type> range_get(const key_type key_lo, const key_type key_hi) = 0;

  virtual std::pair<bool, value_type> background_get_thread(const key_type key) = 0;

  virtual boost::unique_future<std::pair<bool,value_type> > background_get(const key_type key) {
    boost::packaged_task<std::pair<bool, value_type> > pt(boost::bind(&kvstore_base::background_get_thread, this, key));
    boost::detail::thread_move_t<boost::unique_future<std::pair<bool, value_type> > > result = pt.get_future();
    boost::thread t(boost::move(pt));
    t.detach();

    return boost::unique_future<std::pair<bool, value_type> >(result);
  }

  virtual boost::unique_future<std::vector<std::pair<bool, value_type> > > background_bulk_get(const std::vector<key_type> &keys) {
    boost::packaged_task<std::vector<std::pair<bool, value_type> > > pt(boost::bind(&kvstore_base::bulk_get, this, keys));
    boost::detail::thread_move_t<boost::unique_future<std::vector<std::pair<bool, value_type> > > > result = pt.get_future();
    boost::thread t(boost::move(pt));
    t.detach();

    return boost::unique_future<std::vector<std::pair<bool, value_type> > >(result);
  }

  virtual boost::unique_future<std::vector<value_type> > background_range_get(const key_type key_lo, const key_type key_hi) {
    boost::packaged_task<std::vector<value_type> > pt(boost::bind(&kvstore_base::range_get, this, key_lo, key_hi));
    boost::detail::thread_move_t<boost::unique_future<std::vector<value_type> > > result = pt.get_future();
    boost::thread t(boost::move(pt));
    t.detach();

    return boost::unique_future<std::vector<value_type> >(result);
  }

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
