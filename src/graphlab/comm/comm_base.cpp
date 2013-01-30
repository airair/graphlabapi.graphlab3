#include <graphlab/comm/comm_base.hpp>
#include <graphlab/comm/mpi_comm.hpp>
#include <cstring>

namespace graphlab {
 
comm_base* comm_base::create(const char* descriptor,
                             int* argc,
                             char*** argv) {
  if (strcmp(descriptor, "mpi") == 0 || 
      strcmp(descriptor, "MPI") == 0) {
    return new mpi_comm(argc, argv);
  }
  else {
    return NULL;
  }
}

} // namespace graphlab
