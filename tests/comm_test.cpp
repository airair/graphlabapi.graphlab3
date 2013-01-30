#include <string>
#include <cstdlib>
#include <mpi.h>
#include <graphlab/util/timer.hpp>
#include <graphlab/comm/comm_base.hpp>
int main(int argc, char** argv) {
  graphlab::comm_base* comm = graphlab::comm_base::create("mpi", &argc, &argv);
  const char* hello = "hello";
  const char* world = "world";
  std::cout << "Rank: " << comm->rank() << "/" << comm->size() << std::endl;
  comm->send((comm->rank() + 1) % comm->size(), (void*)hello, 5);
  comm->send((comm->rank() + 2) % comm->size(), (void*)world, 5);
  if (comm->rank() == 0) comm->flush(); 
  while(1) {
    int source = 0; size_t length;
    char* ret = (char*)comm->receive(&source, &length);
    if (ret != NULL) {
      std::string s(ret, length);
      std::cout << comm->rank() << ": Received " << s << " from " << source << "\n";
      free(ret);
    } else {
      break;
    }
  }
  comm->flush();
//  comm->barrier();
  while(1) {
    int source = 0; size_t length;
    char* ret = (char*)comm->receive(&source, &length);
    if (ret != NULL) {
      std::string s(ret, length);
      std::cout << comm->rank() << ": Received " << s << " from " << source << "\n";
      free(ret);
    } else {
      break;
    }
  }  

  comm->barrier();
  delete comm;
}
