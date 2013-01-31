#ifndef COMM_RECEIVE_DISPATCH_HPP
#define COMM_RECEIVE_DISPATCH_HPP
#include <graphlab/comm/comm_base.hpp>
#include <graphlab/util/lock_free_pool.hpp>
#include <graphlab/serialization/serialization_includes.hpp>
namespace graphlab {


/**
 * Manages the task of dispatching received messages to
 * message handling functions.
 * Each message has a 2 byte header denoting the message type.
 * And an array associates message types with functions to receive
 * the message type. All message handling functions must support parallel 
 * calls.
 *
 * \note Due to a lack of locking in this implementation, it is important
 * to register ALL handlers before communication is performed. We will implement
 * a mechanism to support this if it becomes necessary.
 */
class comm_rpc {
 public:
  typedef boost::function<void(comm_rpc* comm, 
                               int source, 
                               const char* msg, 
                               size_t len)> dispatch_function_type;
 private:
  comm_base* _comm;

  // memory pool
  lock_free_pool<graphlab::oarchive> _pool;

  // dispatch table
  // boost::unordered_map<unsigned short, dispatch_function > _dispatch;
  std::vector<dispatch_function_type> _dispatch_table;

  void receiver(int machine, const char* c, size_t len);

  bool _comm_has_efficient_send;
 public:
  /**
   * Constructs a rpc which is attached to a comm system.
   * The comm must not already have a receiver attached.
   */
  comm_rpc(comm_base* comm); 

  // destructor
  ~comm_rpc();

  /**
   * Registers a function to handle a message id.
   * Fails if a handler for this message already exists.
   */
  void register_handler(unsigned short message_id,
                        const dispatch_function_type& function);

  /**
   * Returns a pointer to the underlying comm
   */
  inline comm_base* get_comm() {
    return _comm;
  }

  /**
   * Sends a message to a target machine.
   * This function is somewhat less efficient since a copy of the data
   * must be made to attach the message. Using \ref prepare_message and
   * \ref complete_message is more efficient.
   */
  void send_message(int machine, 
                    unsigned short message_id, 
                    const char* data, size_t len);
  /**
   * Returns a graphlab output archive where the contents of the message
   * should be stored into.
   */
  graphlab::oarchive* prepare_message(unsigned short message_id);

  /**
   * Sends out the message to the target machine.
   * arc must be an archive returned by prepare_message
   */
  void complete_message(int machine, graphlab::oarchive* arc); 
};

} // namespace graphlab

#endif
