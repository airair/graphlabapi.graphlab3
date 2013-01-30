#ifndef GRAPHLAB_COMM_COMM_BASE_HPP
#define GRAPHLAB_COMM_COMM_BASE_HPP
#include <cstring>
#include <boost/function.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
namespace graphlab {

/**
 * Abstract base class for a rather basic buffered 
 * communication system.
 *
 * This interface describes an extremely minimal"buffered exchange" system. 
 * Each machine issues messages to other machines through the send() call,
 * And receives messages through the receive() call.
 *
 * The send(), flush() and receive() functions must be thread-safe.
 * However, this interface does not prescribe any particular degree of  
 * parallelism required. i.e. a single mutex locking the entire implementation
 * satisfies the requirements. It should not be possible to deadlock the system
 * under any combination of send(), flush() and receive() calls.
 *
 * No fault tolerance mechanism is defined, and all communication faults 
 * are fatal (for instance, resulting in an assertion failure).
 */ 
class comm_base {
 private:
  // the receiver function
  boost::function<void(int machine, char* c, size_t len)> _receivefun;

  // the receive loop if a receiver is registered
  void receive_loop();
  // the receive thread
  graphlab::thread _receivethread;
  // Signals if the receive thread should die
  volatile bool _done;

 public:
  comm_base():_receivefun(NULL),_done(false) {}; 

  virtual ~comm_base();
  
  static comm_base* create(const char* descriptor,
                           int* argc,
                           char*** argv);
  /**
   * Sends a block of data of some length to a
   * target machine. A copy of the data is made by the comm library.
   * Fails fatally on an error. This function is thread-safe.
   */ 
  virtual void send(int targetmachine, void* data, size_t length) = 0;

  /**
   * Flushes all communication issued prior to this call. Blocks
   * until all communication is complete.
   * This function is thread-safe.
   */
  virtual void flush() = 0;

  /**
   * Receives a copy of a message sent with send() from another machine
   * to this machine.
   * Returns a pointer on success, and NULL if there is no more data to receive.
   * Fails fatally on an error. This function is thread-safe.
   */ 
  virtual void* receive(int* sourcemachine, size_t* length) = 0;

  /**
   * Registers a receive function. This receive function will be called
   * whenever a message is ready to be processed. The message is passed to the
   * function in a form of a character array and a length. The function
   * should not attempt to free this array.
   *
   * If a receive function is registered, calls made to receive() produce 
   * undefined results. The "parallel" parameter is used to inform the 
   * implementation if the receive function may be run in parallel. Setting this 
   * flag does not guarantee that the implementation will perform receives in 
   * parallel; just that it is safe for the implementation to do so: certain 
   * sequential implementations may ignore this flag.
   *
   * Once a receiver is registered, subsequent calls to register_receiver() 
   * has no effect (function returns false). Returns true on success.
   *
   * \note A default implementation using a loop around the receive() call
   * is provided
   */ 
  virtual bool register_receiver(const boost::function<void(int machine, char* c, size_t len)>& receivefun,
                                 bool parallel);

  /**
   * Halts until all machines call the barrier() once.
   */
  virtual void barrier() = 0;

  /**
   * Gets the number of communication nodes
   */
  virtual size_t size() const = 0;
  
  /**
   * Gets the ID of the current node
   */
  virtual size_t rank() const = 0;
};

} // namespace graphlab;
#endif
