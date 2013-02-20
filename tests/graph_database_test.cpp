#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/kvstore_mongodb.cpp>
#include <graphlab/database/kvstore_mysql.cpp>

int main(int argc, char** argv) {
//  graphlab::kvstore_mongodb kv;
  graphlab::kvstore_mysql kv;
  graphlab::key_type k;
  graphlab::value_type v, v1;
  char testv[100];
  char long_key[65000];

  memset(long_key, 1, sizeof(long_key));
  long_key[10000-1] = 0;
  v = long_key;

  for (int i = 0; i < 1000/*00*/; i++) {
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
  printf("Got %d results\n", r.size());

  kv.remove_all();

  r = kv.range_get(0, 10);
  printf("Got %d results\n", r.size());

  return 0;
}
