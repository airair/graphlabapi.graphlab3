#ifndef GRAPHLAB_PARALLEL_QTHREAD_TOOLS_HPP
#define GRAPHLAB_PARALLEL_QTHREAD_TOOLS_HPP
#include <qthread.h>
#include <queue>
#include <boost/function.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
namespace graphlab {

  namespace qthread_tools {
    /** Initializes qthreads with numthreads shephard threads
     * If numthreads is negative, qthreads will autodetect the number of 
     * threads, or use the environment variable QTHREAD_NUM_SHEPHARDS if set.
     *
     * The size of each qthread stack can also be defined in the second 
     * argument. If stacksize is negative, the environment variable 
     * QTHREAD_STACK_SIZE is used. If QTHREAD_STACK_SIZE is not define, 
     * qthread default stacksize is used (qthread default build configuration 
     * is 4K). 
     * 
     * This function may be called multiple times, but only the first call
     * will have any effect.
     */
    void init(int numthreads = -1, int stacksize = -1);
  
  } // qthread_tools



  class qthread_thread {
   private:
    struct invoke_args{
      boost::function<void(void)> spawn_routine;   
      invoke_args(const boost::function<void(void)> &spawn_routine)
          : spawn_routine(spawn_routine) { };
    };
     //! Little helper function used to launch threads
    static aligned_t invoke(void *_args);   

    // return value goes here
    aligned_t retval;
    
    // disable copy constructor
    qthread_thread(const qthread_thread& ) { }
    // disable copy
    qthread_thread& operator=(const qthread_thread& ) { return *this; }

   public:
    qthread_thread();

    /**
     * execute this function to spawn a new thread running spawn_function
     * routine 
     */
    void launch(const boost::function<void (void)> &spawn_routine);

    /**
     * Join the calling thread with this thread.
     */
    void join(); 

    ~qthread_thread();
  };


  /**
   * Defines a thread group. Quite like the \ref thread_group
   * but for fine grained threads. It is a relatively thin wrapper
   * around qthread functions, but provides additional C++ capabilities.
   */  
  class qthread_group {
   private:
     // lock on the return values
     mutex lock;
     std::queue<qthread_thread*> threads;
   public:
     /** 
      * Initializes a thread group. 
      */
     qthread_group();

     /** 
      * Launch a single thread 
      */
     void launch(const boost::function<void (void)> &spawn_function);

     /** Waits for all threads to complete execution. const char* exceptions
       thrown by threads are forwarded to the join() function.
      */
     void join();

     //! Destructor. Waits for all threads to complete execution
     ~qthread_group();

  };


} // namespace graphlab 

#endif
