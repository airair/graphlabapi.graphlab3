#include <graphlab/database/graph_database.hpp>
#include <graphlab/database/kvstore_mongodb.cpp>
#include <graphlab/database/kvstore_mysql.cpp>

int main(int argc, char** argv) {
//  graphlab::kvstore_mongodb kv;
  graphlab::kvstore_mysql kv;
  graphlab::key_type k;
  graphlab::value_type v, v1;
  char testv[100];

  for (int i = 0; i < 100000; i++) {
    k = random() % 1000;
    sprintf(testv, "test%ld", random());
    v = testv;
//    printf("Trying %lld, %s\n", k, v.c_str());
    kv.set(k, v);
    kv.get(k, v1);

//    if (v != v1) {
//      printf("Problem: (%s) (%s)\n", v.c_str(), v1.c_str());
//    } else {
//      printf("Success!\n");
//    }
  }

  return 0;
}
