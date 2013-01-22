#ifndef GRAPHLAB_COMM_MPI_COMM_HPP
#define GRAPHLAB_COMM_MPI_COMM_HPP
#include <climits>
#include <vector>
#include <utility>
#include <sstream>
#include <mpi.h>
#include <boost/shared_ptr.hpp>
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
class mpi_comm : public comm_base{
 private:
  // the size of the mpi group and rank of the current node
  int _size, _rank;

  // the communicator used by MPI calls issued from internal functions
  // of this class (i.e. the background thread)
  MPI_Comm internal_comm;
  // the communicator used by MPI calls issued from public functions
  // of this class
  MPI_Comm external_comm;
  
  // this lock is acquired on flush so only one
  // one thread is flushing at any one time 
  mutex _flush_lock;
  // only one thread is executing the background flush operation sequence
  mutex _background_flush_inner_op_lock;
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
  boost::shared_ptr<int> _buffer_reference_counts[2];
  char __pad2__[64 - 2 * sizeof(boost::shared_ptr<int>)];
  // the base address of the send buffers
  void* _send_base[2];
  // the size of the send window in bytes
  size_t _send_window_size;
  // maximum number of bytes I can send to one machine in a single send
  size_t _max_sendlength_per_machine;
  // offsets into the send buffers
  std::vector<size_t> _offset;
  // offsets into the send buffers scaled by the datatype length
  std::vector<int> _offset_by_datatype;
   // filled lengths into each of the send buffers
  std::vector<atomic<size_t> > _sendlength[2];
  // the last time the buffers were completely reset
  size_t _last_garbage_collect_ms[2];

  // flushing background thread
  bool _flushing_thread_done;
  int _num_nodes_flushing_threads_done;
  thread _flushing_thread;

  // ------- receive buffer management --------
  // The receive buffer is simple. We have a stringbuf
  // per source machine. received data is written into the stringbuf.
  // Once there is a header, the header is parsed and stuck into 
  // next_message_length. Once there is enough contents in the buffer
  // to read the entire message, it is ready.  
  struct receive_buffer_type {
    mutex lock;
    std::stringbuf* buffer;
    size_t buflen;
    size_t next_message_length;
    size_t padded_next_message_length;
  };

  std::vector<receive_buffer_type> _receive_buffer;
  // the actual receive call will sweep between the receive buffers.
  size_t _last_receive_buffer_read_from;

  // -------- private functions -----------

  // construct the send window i
  void construct_send_window(size_t i);

  // destroy the send window i
  void destroy_send_window(size_t i);


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

  // background flusher
  void background_flush();

  // the sequence of MPI operations performed by the background_flush function.
  // by seperating it out into an independent function, I can call it from
  // other sources (such as flush()) while ensuring that the MPI 
  // call sequence still match up perfectly across nodes
  void background_flush_inner_op();

 public:
  mpi_comm(int* argc, char*** argv, 
           size_t send_window = ((size_t)(1) << 32) /* 4GB */);

  ~mpi_comm();
  
  /**
   * Sends a block of data of some length to a
   * target machine. A copy of the data is made by the comm library.
   * Fails fatally on an error. This function is thread-safe.
   */ 
  void send(int targetmachine, void* data, size_t length);

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
   * Halts until all machines hit the barrier_flush() call. All sends
   * issued before this call will also be completed.
   */
  void barrier_flush(); 

  /**
   * Halts until all machines hit the barrier() call
   */
  inline void barrier() {
    MPI_Barrier(external_comm);
  }

  inline size_t size() const {
    return (size_t)_size;
  }
  
  inline size_t rank() const {
    return (size_t)_rank;
  }

};

} // namespace graphlab

#endif
