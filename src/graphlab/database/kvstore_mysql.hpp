/*
 * kvstore_mysql.hpp
 *
 *  Created on: Feb 7, 2013
 *      Author: svilen
 */

#ifndef GRAPHLAB_DATABASE_KVSTORE_MYSQL_HPP
#define GRAPHLAB_DATABASE_KVSTORE_MYSQL_HPP

#include <graphlab/database/kvstore_base.hpp>
#include <mysql5/mysql/storage/ndb/ndbapi/NdbApi.hpp>

namespace graphlab {

const int mysql_max_blob_size = 65535;

class kvstore_mysql: public kvstore_base {
public:
  kvstore_mysql();
  virtual ~kvstore_mysql();

  virtual void set(const key_type key, const value_type &value);

  virtual bool get(const key_type key, value_type &value);
  virtual std::vector<value_type> range_get(const key_type key_lo, const key_type key_hi);

  std::pair<bool, value_type> background_get_thread(const key_type key);

  virtual void remove_all();

private:
  Ndb *_ndb;
  const NdbDictionary::Table *_table;
  const NdbDictionary::Index *_index;
  const NdbDictionary::Dictionary *_dict;
};

}

#endif

