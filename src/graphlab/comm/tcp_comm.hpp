#ifndef GRAPHLAB_TCP_COMM_HPP
#define GRAPHLAB_TCP_COMM_HPP
#include <vector>
#include <string>
#include <boost/function.hpp>
#include <graphlab/comm/comm_base.hpp>
#include <graphlab/comm/tcp/dc_tcp_comm.hpp>
#include <graphlab/comm/tcp/dc_stream_receive.hpp>
#include <graphlab/comm/tcp/dc_buffered_stream_send2.hpp>
namespace graphlab {

/**
 * Implementation of the basic communication system using TCP.
 * Initialization is performed via MPI so MPI is still required
 */ 
class tcp_comm:public comm_base {
 private:
   boost::function<void(int machine, const char* c, size_t len)> _receivefun;
   // used to complete the receive function calls
   thread_group _thread_group;
   bool _dispatch_running;
   size_t _num_threads; 

   // rank of the current machine
   int _rank;
   // size of group
   int _size;
   // the socket handle I am listening on
   int _listen_sockhandle;
   // a list of all the machines
   std::vector<std::string> _machines;

   // the actual comm system
   dc_impl::dc_tcp_comm* comm;
   std::vector<dc_impl::dc_stream_receive*> _receivers;
   std::vector<dc_impl::dc_buffered_stream_send2*> _senders;


   // the actual receive call will sweep between the receive buffers.
   size_t _last_receive_buffer_read_from;

   // Data comes from the receiver in chunks of sequential blocks
   struct chunk{
     char* base;
     char* cur;
     size_t len;
     size_t remaining_len;
     atomic<char> refcount;
     chunk():base(NULL),cur(NULL),len(0),remaining_len(0),refcount(0) { }
     ~chunk() {
       if (base != NULL) free(base);
     }
   };
   // _recv_queue[i] are the messages coming from machine i
   std::vector<std::deque<chunk* > > _recv_queue;
   // this locks the recv_queue
   std::vector<mutex> _recv_queue_lock;
   /** Receives a chunk of stuff */
   void chunk_receive(int machine, char* buf, size_t len);

   // started if a receiver is registered
   void receiver_thread(size_t threadid);
   std::vector<mutex> _thread_mutex;
   std::vector<conditional> _thread_cond;
   std::vector<char> _thread_trying_to_sleep;
   char* advance_chunk(chunk* c, size_t* recvlen);
   // received from a specific source machine
   void* receive(int sourcemachine, size_t* length);
 public:
  tcp_comm(int* argc, char*** argv);

  ~tcp_comm();


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
   * Registers a receive function. 
   */
  bool register_receiver(const boost::function<void(int machine, const char* c, size_t len)>& receivefun,
                         bool parallel);

  /**
   * Halts until all machines hit the barrier() call
   */
  void barrier();

  int size() const {
    return _size;
  }
  
  int rank() const {
    return _rank;
  }

  inline bool has_efficient_send() const {
    return false;
  }

};

} // namespace graphlab;

#endif
