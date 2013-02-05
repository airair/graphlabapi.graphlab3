/*
 * kvstore_mongodb.cpp
 *
 *  Created on: Feb 4, 2013
 *      Author: svilen
 */

#include <memory>

#include <graphlab/database/kvstore_base.hpp>
#include <graphlab/logger/assertions.hpp>
#include <mongo/client/dbclient.h>
#include <mongo/util/net/hostandport.h>
#include <boost/foreach.hpp>

#define KEYATTR_NAME "_id"
#define VALUEATTR_NAME "value"

namespace graphlab {

kvstore_mongodb::kvstore_mongodb(std::string addr, int port, std::string ns) : _ns(ns) {
  std::string error_msg;
  ASSERT_TRUE(_conn.connect(mongo::HostAndPort(addr, port), error_msg));
}

kvstore_mongodb::~kvstore_mongodb() {
}

void kvstore_mongodb::set(const key_type key, const value_type &value) {
  _conn.update(_ns, BSON(KEYATTR_NAME << key), BSON(KEYATTR_NAME << key << VALUEATTR_NAME << value), true);
}

void kvstore_mongodb::background_set(const key_type key, const value_type &value) {
  set(key, value);
}

bool kvstore_mongodb::get(const key_type key, value_type &value) {
  mongo::BSONObj query = BSON(KEYATTR_NAME << key);
  mongo::BSONObj result = _conn.findOne(_ns, query);
  if (result.isEmpty()) {
    return false;
  }

  value = result.getStringField(VALUEATTR_NAME);
  return true;
}

std::vector<std::pair<bool, value_type> > kvstore_mongodb::bulk_get(const std::vector<key_type> &keys) {
  std::vector<std::pair<bool, value_type> > result;
  bool r;
  value_type v;
  BOOST_FOREACH(key_type key, keys) {
    r = get(key, v);
    result.push_back(std::pair<bool, value_type>(r, v));
  }
  return result;
}

std::vector<value_type> kvstore_mongodb::range_get(const key_type key_lo, const key_type key_hi) {
  mongo::BSONObj query = BSON(KEYATTR_NAME << mongo::GTE << key_lo << mongo::LT << key_hi);
  std::vector<value_type> result;
  std::auto_ptr<mongo::DBClientCursor> cursor = _conn.query(_ns, query);
  while (cursor->more()) {
    result.push_back(cursor->next().getStringField(VALUEATTR_NAME));
  }
  return result;
}

boost::unique_future<std::pair<bool, value_type> > kvstore_mongodb::background_get(const key_type key) {
  boost::unique_future<std::pair<bool, value_type> > result;
  return result;
}

boost::unique_future<std::vector<std::pair<bool, value_type> > > kvstore_mongodb::background_bulk_get(const std::vector<key_type> &keys) {
  boost::unique_future<std::vector<std::pair<bool, value_type> > > result;
  return result;
}

boost::unique_future<std::vector<value_type> > kvstore_mongodb::background_range_get(const key_type key_lo, const key_type key_hi) {
  boost::unique_future<std::vector<value_type> > result;
  return result;
}

}

