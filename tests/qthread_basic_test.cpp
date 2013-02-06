#include <iostream>
#include <graphlab/parallel/qthread_tools.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
#include <qthread/qtimer.h>
#include <boost/bind.hpp>
#include <cstdlib>
void test_print_qthread(size_t i) {
  qtimer_t t = qtimer_create();
  qtimer_start(t);
  do {
    qthread_yield();
    qtimer_stop(t);
  } while(qtimer_secs(t) < 1);
  qtimer_destroy(t);
  //std::cout << i << "\n";
}

void test_print_pthread_busy_sleep(size_t i) {
  qtimer_t t = qtimer_create();
  qtimer_start(t);
  do {
    sched_yield();
    qtimer_stop(t);
  } while(qtimer_secs(t) < 1);
  qtimer_destroy(t);
  //std::cout << i << "\n";
}



void test_print_pthread(size_t i) {
  graphlab::timer::sleep(1);
  //std::cout << i << "\n";
}



int main(int argc, char** argv) {
  size_t numthreads = 10000;
  if (argc != 2) {
    std::cout << "First argument is # threads to spawn. Recommended 10K\n";
    exit(1);
  }
  numthreads = atoi(argv[1]);

  std::cout << "This test spawns " << numthreads 
            << " threads, each sleeping for 1 sec.\n";

  std::cout << "Busy Pthread sleep: \n";
  graphlab::timer ti; ti.start();
  {
    graphlab::thread_group group;
    for (size_t i = 0;i < numthreads; ++i) {
      group.launch(boost::bind(test_print_pthread_busy_sleep, i));
    }
    group.join();  
  }
  std::cout << "Busy PThread in " << ti.current_time() << " seconds\n";
  
  std::cout << "Qthread sleep: \n";
  ti.start();
  {
    graphlab::qthread_group group;
    for (size_t i = 0;i < numthreads; ++i) {
      group.launch(boost::bind(test_print_qthread, i));
    }
    group.join();  
  }
  std::cout << "QThread in " << ti.current_time() << " seconds\n";

  std::cout << "Regular Pthread sleep (note: this may crash for large thread numbers): \n";
  ti.start();
  {
    graphlab::thread_group group;
    for (size_t i = 0;i < numthreads; ++i) {
      group.launch(boost::bind(test_print_pthread, i));
    }
    group.join();  
  }
  std::cout << "Regular PThread in " << ti.current_time() << " seconds\n";
}

