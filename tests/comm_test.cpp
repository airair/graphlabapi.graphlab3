#include <string>
#include <cstdlib>
#include <mpi.h>
#include <graphlab/util/timer.hpp>
#include <graphlab/comm/comm_base.hpp>
int main(int argc, char** argv) {
  graphlab::comm_base* comm ;
  if (argc > 1) {
    comm = graphlab::comm_base::create(argv[1], &argc, &argv);
  } else { 
    comm = graphlab::comm_base::create("mpi", &argc, &argv);
  }
  assert(comm != NULL);
  const char* hello = "hello";
  const char* world = "world";
  std::cout << "Rank: " << comm->rank() << "/" << comm->size() << std::endl;
  comm->send((comm->rank() + 1) % comm->size(), (void*)hello, 5);
  comm->send((comm->rank() + 2) % comm->size(), (void*)world, 5);
  comm->flush(); 
  size_t ctr = 0;
  while(ctr != 2) {
    int source = 0; size_t length;
    char* ret = (char*)comm->receive(&source, &length);
    if (ret != NULL) {
      std::string s(ret, length);
      std::cout << comm->rank() << ": Received " << s << " from " << source << "\n";
      free(ret);
      ++ctr;
    } 
    usleep(10);
  }  

  comm->barrier();
  delete comm;
}
