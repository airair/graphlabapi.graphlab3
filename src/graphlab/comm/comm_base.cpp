#include <cstring>
#include <boost/bind.hpp>
#include <graphlab/comm/comm_base.hpp>
#include <graphlab/comm/mpi_comm.hpp>
#include <graphlab/comm/mpi_comm2.hpp>
#include <graphlab/comm/tcp_comm.hpp>

namespace graphlab {
 
comm_base* comm_base::create(const char* descriptor,
                             int* argc,
                             char*** argv) {
  comm_base* ret = NULL;
  if (strcmp(descriptor, "mpi") == 0 || 
      strcmp(descriptor, "MPI") == 0) {
    ret = new mpi_comm(argc, argv);
    if (ret->rank() == 0) std::cout << "MPI Communicator constructed\n";
  } else if (strcmp(descriptor, "mpi2") == 0 || 
      strcmp(descriptor, "MPI2") == 0) {
    ret = new mpi_comm2(argc, argv);
    if (ret->rank() == 0) std::cout << "MPI Communicator constructed\n";
  } else if (strcmp(descriptor, "tcp") == 0 || 
      strcmp(descriptor, "TCP") == 0) {
    ret = new tcp_comm(argc, argv);
    if (ret->rank() == 0) std::cout << "TCP Communicator constructed\n";
  } else {
    std::cout << "Unknown Communicator type: " << descriptor << "\n";
  }
  return ret;
}


bool comm_base::register_receiver(
    const boost::function<void(int machine, const char* c, size_t len)>& receivefun,
    bool parallel) {
  if (_receivefun == NULL) {
    _receivefun = receivefun; 
    // now we need to launch a thread to perform the receive
    _done = false;
    _receivethread.launch(boost::bind(&comm_base::receive_loop, this));
    return true;
  } else {
    return false;
  }
}

void comm_base::receive_loop() {
  while(!_done) {
    int source; size_t len;
    void* msg = NULL;
    do {
      msg = receive(&source, &len);
      if (msg != NULL) {
        _receivefun(source, (char*)(msg), len);
        free(msg);
      }
    } while(!_done && msg != NULL);
    usleep(1000);
  }
}



comm_base::~comm_base() {
  if (_receivefun != NULL) {
    _done = true;
    _receivethread.join();
  }
}

} // namespace graphlab
