#include <graphlab/database/sharedmem_database/graph_database_sharedmem.hpp>
#include <graphlab/database/graph_database_server.hpp>
#include <graphlab/logger/assertions.hpp>
#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>
#include <boost/lexical_cast.hpp>
#include <graphlab/database/queryobj.hpp>
#include <fault/query_object_server.hpp>
#include <boost/lexical_cast.hpp>
#include "graph_database_test_util.hpp"
#include <vector>
using namespace std;
