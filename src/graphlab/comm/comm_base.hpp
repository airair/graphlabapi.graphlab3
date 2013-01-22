#ifndef GRAPHLAB_COMM_COMM_BASE_HPP
#define GRAPHLAB_COMM_COMM_BASE_HPP
#include <cstring>
namespace graphlab {

/**
 * Abstract base class for a rather basic buffered 
 * communication system.
 *
 * This interface describes an extremely minimal "buffered exchange" system. 
 * Each machine issues messages to other machines through the send() call,
 * And receives messages through the receive() call.
 *
 * All functions must be thread-safe, but depending on the design of the 
 * underlying comm, certain operations may block the execution of other 
 * operations. However, it should not be possible to deadlock the system.
 *
 * No fault tolerance mechanism is defined, and all communication faults 
 * are fatal (for instance, resulting in an assertion failure).
 */ 
class comm_base {
 public:
  comm_base() {}; 
  virtual ~comm_base() {};
  
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
