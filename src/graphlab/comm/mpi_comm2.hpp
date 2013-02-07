#ifndef GRAPHLAB_COMM_mpi_comm22_HPP
#define GRAPHLAB_COMM_mpi_comm22_HPP
#include <climits>
#include <vector>
#include <utility>
#include <sstream>
#include <mpi.h>
#include <graphlab/util/circular_char_buffer.hpp>
#include <graphlab/comm/comm_base.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
#include <graphlab/parallel/atomic.hpp>
namespace graphlab {

/** An implementation of the generic comm system using MPI.
 * The basic design is to use a background thread with a loop around
 * MPI_Alltoallv, continuously flushing the communication channels
 * once total available communication exceeds certain thresholds.
 * The flush() call simply forces the Alltoallv to complete.
 */
class mpi_comm2 : public comm_base{
 private:
  // the size of the mpi group and rank of the current node
  int _size, _rank;

  // the communicator used by MPI calls issued from internal functions
  // of this class (i.e. the background thread)
  MPI_Comm internal_comm;
  // the communicator used by MPI calls issued from public functions
  // of this class
  MPI_Comm external_comm;

  // ------- send buffer management --------
  // we manage 2 send buffers and swap between them

  // the current send buffer send calls write to. This
  // counter only increments. thus _cur_send_buffer % 2 is the 
  // index of the send buffer to write to
  atomic<size_t> _cur_send_buffer;
  char __pad1__[64 - sizeof(atomic<size_t>)];

  // we use boost shared pointers to manage the reference counts
  // to each send buffer. We stick some arbitary int* into the shared
  // pointer to keep it happy
  atomic<size_t> _buffer_reference_counts[2];
 
  // a list of all the mmaped buffers. 
  void* _mmaped_buffers[3];
  // the base address of the send buffers. These are from the mmaped_buffers
  void* _send_base[2];
  // the base address of the receive buffer. These are from the mmaped_buffers
  void* _recv_base;
  // the size of the send window in bytes
  size_t _send_window_size;
  // maximum number of bytes I can send to one machine in a single send
  size_t _max_sendlength_per_machine;
  // offsets into the send buffers
  std::vector<size_t> _offset;
   // filled lengths into each of the send buffers
  std::vector<atomic<size_t> > _sendlength[2];
  // the last time the buffers were completely reset
  size_t _last_garbage_collect_ms[2];

  // only one thread can be sending to a particular target machine at any
  // one time. 
  std::vector<mutex> _send_locks;

  // receiving background thread
  bool _receive_thread_done;
  thread _receive_thread;
  // used to mark that the thread is in (or not in) MPI_Wait
  // When it is in MPI_Wait, it will release the lock
  mutex _receive_thread_lock;

  // flushing background thread
  bool _send_thread_done;
  thread _send_thread;
  mutex _flush_lock;
  conditional _flush_cond;

  size_t _bytes_sent;
  size_t _bytes_received;

  // ------- receive buffer management --------
  // The receive buffer is simple. We have a stringbuf
  // per source machine. received data is written into the stringbuf.
  // Once there is a header, the header is parsed and stuck into 
  // next_message_length. Once there is enough contents in the buffer
  // to read the entire message, it is ready.  
  struct receive_buffer_type {
    mutex lock;
    circular_char_buffer buffer;
    size_t buflen;
    size_t next_message_length;
  };

  std::vector<receive_buffer_type> _receive_buffer;
  // the actual receive call will sweep between the receive buffers.
  size_t _last_receive_buffer_read_from;

  // Set to true if a receive function was registered
  bool _has_receiver;
  // The receiver function.
  boost::function<void(int machine, const char* c, size_t len)> _receivefun;
  // true if the the receive function is parallel
  bool _parallel_receiver;
  mutex _receiver_lock;

  // -------- private functions -----------

  // construct the send window i
  void construct_window(size_t i);

  // destroy the send window i
  void destroy_window(size_t i);


  // the actual send function. Returns the number of bytes sent. 
  // Send may be incomplete
  size_t actual_send(int targetmachine, void* data, size_t length); 

  // The actual flush implementation. Assumes that only one thread is here
  // at any point. Sends a particular send buffer.
  void actual_flush(size_t idx, MPI_Comm communicator);

  // exchange the send buffers. Assumes that only only one thread is here
  // at any point. Returns the index of the previous buffer
  size_t swap_buffers();

  // unmaps and remaps the buffer to clear any allocated pages
  void garbage_collect(size_t idx);

  // clears the send buffer idx so it can be reused
  // also "garbage collects" it every 10 seconds
  void reset_send_buffer(size_t idx);

  // writes this number of bytes into the receive buffer from machine idx
  void insert_receive_buffer(size_t idx, char* v, size_t length);

  // requires buffer to be locked. Read the header from buffer idx 
  // and fill the next_message_length values
  void locked_read_header_from_buffer(size_t idx);

  // background send thread 
  void send_thread_function();

  // background receive thread
  void receive_thread_function();


  void halt_threads();

  /*
   * Performs a receive, but calls the receiver function on the result.
   * Returns the number of receive calls made
   */ 
  size_t receiver_fun_receive(int sourcemachine);

  /*
   * Performs a receive, but calls the receiver function on the result.
   * Returns the number of receive calls made
   */ 
  size_t receiver_fun_receive();

 public:
  mpi_comm2(int* argc, char*** argv, 
           size_t send_window = ((size_t)(1) << 32) /* 4GB */);

  ~mpi_comm2();
  
  /**
   * Sends a block of data of some length to a
   * target machine. A copy of the data is made by the class.
   * Fails fatally on an error. This function is thread-safe.
   */ 
  void send(int targetmachine, void* data, size_t length);

/**
   * Sends a block of data of some length to a
   * target machine. The comm will free the pointer when done.
   * Fails fatally on an error. This function is thread-safe.
   */ 
  void send_relinquish(int targetmachine, void* data, size_t length);


  /**
   * Flushes all communication issued prior to this call. Blocks
   * until all communication is complete.
   * This function is thread-safe.
   */
  void flush(); 

  /**
   * Receives a copy of a message sent with send() from another machine
   * to this machine.
   * Returns a pointer on success, and NULL if there is no more data to receive.
   * Fails fatally on an error. This function is thread-safe.
   */ 
  void* receive(int* sourcemachine, size_t* length); 


  /**
   * Receives a copy of a message sent with send() from a particular source
   * machine to this machine.
   * Returns a pointer on success, and NULL if there is no more data to receive.
   * Fails fatally on an error. This function is thread-safe.
   */ 
  void* receive(int sourcemachine, size_t* length); 


  /**
   * Registers a receive function. The parallel flag is ignored.
   */
  bool register_receiver(const boost::function<void(int machine, const char* c, size_t len)>& receivefun,
                         bool parallel);



  /**
   * Halts until all machines hit the barrier() call
   */
  void barrier();

  inline int size() const {
    return (size_t)_size;
  }
  
  inline int rank() const {
    return (size_t)_rank;
  }


  inline bool has_efficient_send() const {
    return true;
  }

};

} // namespace graphlab

#endif
