#include <boost/date_time/posix_time/posix_time_types.hpp>

#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/kvstore_mongodb.hpp>
#include <graphlab/database/kvstore_mysql.hpp>
#include <graphlab/database/kvstore_base.hpp>

void test_kv(graphlab::kvstore_base &kv) {
  graphlab::key_type k;
  graphlab::value_type v, v1;
  char testv[100];
  char long_key[65000];

  memset(long_key, 1, sizeof(long_key));
  long_key[2000-1] = 0;
  v = long_key;

  for (int i = 0; i < 100000/*00*/; i++) {
    k = random() % 1000;
    sprintf(testv, "test%ld", random());
    v = testv;

//    printf("Trying %lld, %s\n", k, v.c_str());

    kv.set(k, v);
    kv.get(k, v1);

    if (v != v1) {
      printf("Problem: (%s) (%s)\n", v.c_str(), v1.c_str());
    } else {
//      printf("Success!\n");
    }
  }

  std::vector<graphlab::value_type> r;

  r = kv.range_get(0, 10);
  printf("Got %ld results\n", r.size());

  kv.remove_all();

  r = kv.range_get(0, 10);
  printf("Got %ld results\n", r.size());
}

typedef boost::posix_time::ptime Time;
typedef boost::posix_time::time_duration TimeDuration;

int main(int argc, char** argv) {
  graphlab::kvstore_mongodb kv_mongo;
  graphlab::kvstore_mysql kv_mysql;

  Time t1, t2;
  TimeDuration td;

  printf("Runnung mongo test...\n");
  t1 = boost::posix_time::microsec_clock::local_time();
  test_kv(kv_mongo);
  t2 = boost::posix_time::microsec_clock::local_time();
  printf("Finished in %lld ms.\n", (t2-t1).total_milliseconds());

  printf("Runnung mysql test...\n");
  t1 = boost::posix_time::microsec_clock::local_time();
  test_kv(kv_mysql);
  t2 = boost::posix_time::microsec_clock::local_time();
  printf("Finished in %lld ms.\n", (t2-t1).total_milliseconds());

  return 0;
}
