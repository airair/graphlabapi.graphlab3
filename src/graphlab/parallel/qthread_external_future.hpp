#ifndef QTHREAD_EXTERNAL_FUTURE_HPP
#define QTHREAD_EXTERNAL_FUTURE_HPP
#include <boost/function.hpp>
#include <qthread.h>
namespace graphlab {

/**
 * Used by qthread functions to invoke an external operation
 * and to wait for a response.
 * The standard pattern is as follows
 *
 * /code
 * qthread_fn() {
 *   // create a future
 *   qthread_external_future<int> future;
 *   size_t handle = future.get_handle();
 *   int* result_ptr = future.get_result_ptr(); 
 *   __call_external_function_to_do_stuff__(handle, result_ptr);
 *   // will wait until signal is raised on the future
 *   T* result = future.wait();
 * }
 *
 * void do_stuff(void* handle, int* result) {
 *    .. doo stufff
 *    .. perhaps even give the handle and result pointers away and
 *    .. return. 
 *    .. but eventually, someone somewhere must assign the result value 
 *    .. and call qthread_external_future<int>::signal(handle)
 * }
 *
 *
 * /endcode
 */
template <typename T>
class qthread_external_future {
 private:
  aligned_t _var;
  T _response;
 public:
  qthread_external_future() {
    // empty the var
    qthread_empty(&_var);
  }

  /** Gets the handle to signal on. 
   *  To signal, signal_future(handle)
   */
  size_t get_handle() {
    return reinterpret_cast<size_t>((void*)(&_var));
  }

  /** Gets the address of the result of the future.
   * This should be passed together with the handle and should be 
   * set before signal_future() is called.
   */
  T* get_result_ptr() {
    return &_response;
  }

  /**
   * signals that a thread waiting on this future should wake up
   */
  static void signal(size_t handle) {
    qthread_fill(reinterpret_cast<aligned_t*>(handle)); 
  }

  /**
   * Waits for this future to be filled.
   */
  T* wait() {
    qthread_readFF(NULL, &_var);
    return &_response;
  }
}; 

} // namespace graphlab
#endif
