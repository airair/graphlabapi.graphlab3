#include <cstring>
#include <boost/bind.hpp>
#include <graphlab/comm/comm_base.hpp>
#include <graphlab/comm/mpi_comm.hpp>

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
      _receivefun(source, (char*)(msg), len);
      free(msg);
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
