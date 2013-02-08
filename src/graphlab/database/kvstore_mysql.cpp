/*
 * kvstore_mysql.cpp
 *
 *  Created on: Feb 7, 2013
 *      Author: svilen
 */

#include <graphlab/database/kvstore_mysql.hpp>
#include <graphlab/logger/assertions.hpp>
#include <boost/foreach.hpp>
#include <boost/thread.hpp>
#include <boost/move/move.hpp>

#include <mysql5/mysql/mysql.h>
#include <mysql5/mysql/mysqld_error.h>
#include <mysql5/mysql/my_global.h>
//#include <mysql5/mysql/storage/ndb/ndb_init.h>

namespace graphlab {

const std::string mysql_db_name = "graphlab_db";
const std::string mysql_table_name = "graphlab_kv";
const char* mysql_default_connstr = "localhost:1186";
const char* mysql_default_sock = "/tmp/mysql.sock";
const char* mysql_default_addr = "localhost";
const char* mysql_default_user = "root";

const char* mysql_keyattr_name = "ID";
const char* mysql_valueattr_name = "VAL";

kvstore_mysql::kvstore_mysql() {
  ndb_init();
  MYSQL mysql;

  ASSERT_TRUE(mysql_init(&mysql));
  ASSERT_TRUE(mysql_real_connect(&mysql, mysql_default_addr, mysql_default_user, "", "", 0, mysql_default_sock, 0));

  mysql_query(&mysql, ("CREATE DATABASE "+mysql_db_name).c_str());
  ASSERT_FALSE(mysql_query(&mysql, ("USE "+mysql_db_name).c_str()));

  mysql_query(&mysql, ("CREATE TABLE "+mysql_table_name+" ("+mysql_keyattr_name+" INT UNSIGNED NOT NULL PRIMARY KEY, "+
                                                           mysql_valueattr_name+" BLOB NOT NULL) ENGINE=NDB").c_str());

  Ndb_cluster_connection cluster_connection(mysql_default_connstr);
  ASSERT_FALSE(cluster_connection.connect(4, 5, 1));
  ASSERT_FALSE(cluster_connection.wait_until_ready(30, 0));

  _ndb = new Ndb(&cluster_connection, mysql_db_name.c_str());
  ASSERT_FALSE(_ndb->init(1024));

  const NdbDictionary::Dictionary* dict = _ndb->getDictionary();
  _table = dict->getTable("api_simple");
}

kvstore_mysql::~kvstore_mysql() {
  ndb_end(0);
  delete _ndb;
  _ndb = NULL;
}

void kvstore_mysql::set(const key_type key, const value_type &value) {
  ASSERT_TRUE(_table != NULL);

  NdbTransaction *trans = _ndb->startTransaction();
  ASSERT_TRUE(trans != NULL);

  NdbOperation *op = trans->getNdbOperation(_table);
  ASSERT_TRUE(op != NULL);

  op->updateTuple();
  op->equal(mysql_keyattr_name, (int) key);
  op->setValue(mysql_valueattr_name, value.c_str());

  ASSERT_FALSE(trans->execute(NdbTransaction::Commit) == -1);

  _ndb->closeTransaction(trans);
}

void kvstore_mysql::background_set(const key_type key, const value_type &value) {
  boost::thread t(&kvstore_base::set, this, key, value);
  t.detach();
}

bool kvstore_mysql::get(const key_type key, value_type &value) {
  return false;
}

std::vector<std::pair<bool, value_type> > kvstore_mysql::bulk_get(const std::vector<key_type> &keys) {
  std::vector<std::pair<bool, value_type> > result;
  return result;
}

std::vector<value_type> kvstore_mysql::range_get(const key_type key_lo, const key_type key_hi) {
  std::vector<value_type> result;
  return result;
}

std::pair<bool, value_type> kvstore_mysql::background_get_thread(const key_type key) {
  return std::pair<bool, value_type>();
}

boost::unique_future<std::pair<bool, value_type> > kvstore_mysql::background_get(const key_type key) {
  boost::packaged_task<std::pair<bool, value_type> > pt(boost::bind(&kvstore_mysql::background_get_thread, this, key));
  boost::detail::thread_move_t<boost::unique_future<std::pair<bool, value_type> > > result = pt.get_future();
  boost::thread t(boost::move(pt));
  t.detach();

  return boost::unique_future<std::pair<bool, value_type> >(result);
}

boost::unique_future<std::vector<std::pair<bool, value_type> > > kvstore_mysql::background_bulk_get(const std::vector<key_type> &keys) {
  boost::packaged_task<std::vector<std::pair<bool, value_type> > > pt(boost::bind(&kvstore_base::bulk_get, this, keys));
  boost::detail::thread_move_t<boost::unique_future<std::vector<std::pair<bool, value_type> > > > result = pt.get_future();
  boost::thread t(boost::move(pt));
  t.detach();

  return boost::unique_future<std::vector<std::pair<bool, value_type> > >(result);
}

boost::unique_future<std::vector<value_type> > kvstore_mysql::background_range_get(const key_type key_lo, const key_type key_hi) {
  boost::packaged_task<std::vector<value_type> > pt(boost::bind(&kvstore_base::range_get, this, key_lo, key_hi));
  boost::detail::thread_move_t<boost::unique_future<std::vector<value_type> > > result = pt.get_future();
  boost::thread t(boost::move(pt));
  t.detach();

  return boost::unique_future<std::vector<value_type> >(result);
}

void kvstore_mysql::remove_all() {

}

}
