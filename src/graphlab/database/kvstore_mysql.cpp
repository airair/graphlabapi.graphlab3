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
#include <mysql5/mysql/storage/ndb/ndbapi/NdbApi.hpp>
#include <mysql5/mysql/storage/ndb/ndb_init.h>

namespace graphlab {

const char* mysql_db_name = "graphlab_db";
const char* mysql_table_name = "graphlab_kv";
const char* mysql_default_connstr = "localhost:1186";
const char* mysql_default_sock = "/tmp/mysql.sock";
const char* mysql_default_addr = "localhost";
const char* mysql_default_user = "root";

const char* mysql_keyattr_name= "ID";
const char* mysql_valueattr_name = "VAL";

kvstore_mysql::kvstore_mysql() {
  ndb_init();
  MYSQL mysql;

  ASSERT_TRUE(mysql_init(&mysql));
  ASSERT_TRUE(mysql_real_connect(&mysql, mysql_default_addr, mysql_default_user, "", "", 0, mysql_default_sock, 0));

  mysql_query(&mysql, "CREATE DATABASE "+mysql_db_name);
  ASSERT_FALSE(mysql_query(&mysql, "USE "+mysql_db_name));

  mysql_query(&mysql, "CREATE TABLE "+mysql_table_name+" ("+mysql_keyattr_name+" INT UNSIGNED NOT NULL PRIMARY KEY, "+
                                                           mysql_valueattr_name+" BLOB NOT NULL) ENGINE=NDB");

  Ndb_cluster_connection cluster_connection(mysql_default_connstr);
  ASSERT_FALSE(cluster_connection.connect(4, 5, 1));
  ASSERT_FALSE(cluster_connection.wait_until_ready(30, 0));

  Ndb myNdb(&cluster_connection, mysql_db_name);
  ASSERT_FALSE(myNdb.init(1024));
}

}
