/*
 * kvstore_mongodb.cpp
 *
 *  Created on: Feb 4, 2013
 *      Author: svilen
 */

#include <memory>

#include <graphlab/database/kvstore_mongodb.hpp>
#include <graphlab/logger/assertions.hpp>

#include <client/dbclient.h>
#include <mongo/util/net/hostandport.h>

namespace graphlab {

const char* mongodb_keyattr_name= "_id";
const char* mongodb_valueattr_name = "value";

const std::string MONGODB_DEFAULT_ADDR = "127.0.0.1";
const int MONGODB_DEFAULT_PORT = 27017;
const std::string MONGODB_DEFAULT_NAMESPACE = "graphlab.graphlab";


kvstore_mongodb::kvstore_mongodb(std::string addr, int port, std::string ns) : _ns(ns) {
  std::string error_msg;
  ASSERT_TRUE(_conn.connect(mongo::HostAndPort(addr, port), error_msg));
  _conn.createCollection(_ns);
  printf("MongoDB connection open\n");
}

kvstore_mongodb::~kvstore_mongodb() {
  printf("MongoDB connection closed\n");
}

void kvstore_mongodb::remove_all() {
  _conn.dropCollection(_ns);
  _conn.createCollection(_ns);
}

void kvstore_mongodb::set(const key_type key, const value_type &value) {
  _conn.update(_ns, BSON(mongodb_keyattr_name << (long long) key), BSON(mongodb_keyattr_name << (long long) key << mongodb_valueattr_name << value), true);
}

bool kvstore_mongodb::get(const key_type key, value_type &value) {
  mongo::BSONObj query = BSON(mongodb_keyattr_name << (long long) key);
  mongo::BSONObj result = _conn.findOne(_ns, query);
  if (result.isEmpty()) {
    return false;
  }

  value = result.getStringField(mongodb_valueattr_name);
  return true;
}

std::vector<value_type> kvstore_mongodb::range_get(const key_type key_lo, const key_type key_hi) {
  mongo::BSONObj query = BSON(mongodb_keyattr_name << mongo::GTE << (long long) key_lo << mongo::LT << (long long) key_hi);
  std::vector<value_type> result;
  std::auto_ptr<mongo::DBClientCursor> cursor = _conn.query(_ns, query);
  while (cursor->more()) {
    result.push_back(cursor->next().getStringField(mongodb_valueattr_name));
  }
  return result;
}

std::pair<bool, value_type> kvstore_mongodb::background_get_thread(const key_type key) {
  mongo::BSONObj query = BSON(mongodb_keyattr_name << (long long) key);
  mongo::BSONObj result = _conn.findOne(_ns, query);
  bool empty = result.isEmpty();
  value_type value;
  if (!empty)
    value = result.getStringField(mongodb_valueattr_name);
  return std::pair<bool, value_type>(empty, value);
}

}

