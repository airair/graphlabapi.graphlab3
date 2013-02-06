/*
 * kvstore_mongodb.cpp
 *
 *  Created on: Feb 4, 2013
 *      Author: svilen
 */

#ifndef GRAPHLAB_DATABASE_KVSTORE_MONGODB.HPP
#define GRAPHLAB_DATABASE_KVSTORE_MONGODB.HPP

#include <mongo/client/dbclient.h>

namespace graphlab {

#define MONGODB_DEFAULT_ADDR "127.0.0.1"
#define MONGODB_DEFAULT_PORT 27017
#define MONGODB_DEFAULT_NAMESPACE "graphlab"

class kvstore_mongodb: public kvstore_base {
public:
  kvstore_mongodb(): kvstore_mongodb(MONGODB_DEFAULT_ADDR, MONGODB_DEFAULT_PORT, MONGODB_DEFAULT_NAMESPACE) {}
  kvstore_mongodb(std::string addr, int port, std::string ns);
  virtual ~kvstore_mongodb();

  virtual void set(const key_type key, const value_type &value);
  virtual void background_set(const key_type key, const value_type &value);

  virtual bool get(const key_type key, value_type &value);
  virtual std::vector<std::pair<bool, value_type> > bulk_get(const std::vector<key_type> &keys);
  virtual std::vector<value_type> range_get(const key_type key_lo, const key_type key_hi);

  virtual boost::unique_future<std::pair<bool, value_type> > background_get(const key_type key);
  virtual boost::unique_future<std::vector<std::pair<bool, value_type> > > background_bulk_get(const std::vector<key_type> &keys);
  virtual boost::unique_future<std::vector<value_type> > background_range_get(const key_type key_lo, const key_type key_hi);

private:
  mongo::DBClientConnection _conn;
  std::string _ns;

  void background_get_thread(boost::promise<std::pair<bool, value_type> > promise, const key_type key);
};

}

#endif

