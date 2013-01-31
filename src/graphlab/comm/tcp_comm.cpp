#include <iostream>
#include <string>
#include <vector>
#include <boost/bind.hpp>
#include <graphlab/util/net_util.hpp>
#include <graphlab/logger/logger.hpp>
#include <graphlab/comm/tcp_comm.hpp>
#include <graphlab/util/stl_util.hpp>
#include <graphlab/comm/tcp/packet_header.hpp>
#include <mpi.h>
#include <graphlab/util/mpi_tools.hpp>

namespace graphlab {

tcp_comm::tcp_comm(int* argc, char*** argv) {
  // we need MPI to start
  mpi_tools::init(*argc, *argv, 0);
// ----- Initialization. Set up the rank, size and get a list of the machines
  // Look for a free port to use. 
  std::pair<size_t, int> port_and_sock = get_free_tcp_port();
  size_t port = port_and_sock.first;
  _listen_sockhandle = port_and_sock.second;
  
  std::string ipaddr = get_local_ip_as_str();
  ipaddr = ipaddr + ":" + tostr(port);
  // now do an allgather
  logstream(LOG_INFO) << "Will Listen on: " << ipaddr << std::endl;
  mpi_tools::all_gather(ipaddr, _machines);
  // set defaults
  _rank = mpi_tools::rank();
  _size = mpi_tools::size();

// ----- now set up the receive data structures
  _last_receive_buffer_read_from = 0;
  _recv_queue.resize(_size);
  _recv_queue_lock.resize(_size);
  _dispatch_running = false;
// ---- now set up the comm system

  comm = new dc_impl::dc_tcp_comm();
  
  // construct the send and receive buffers
  for (size_t i = 0; i < _machines.size(); ++i) {
    _receivers.push_back(new dc_impl::dc_stream_receive(
            boost::bind(&tcp_comm::chunk_receive, this, _1, _2, _3),
            i));
    _senders.push_back(new dc_impl::dc_buffered_stream_send2(comm, _rank, i));
  }
  // initialize comm
  std::map<std::string, std::string> options;
  options["__sockhandle__"] = tostr(_listen_sockhandle);
  comm->init(_machines, options, _rank, 
             _receivers, _senders);
 
}


tcp_comm::~tcp_comm() {
  for (size_t i = 0;i < _senders.size(); ++i) {
    _senders[i]->flush();
  }
  comm->close();
  for (size_t i = 0;i < _senders.size(); ++i) {
    delete _senders[i];
  }
  for (size_t i = 0;i < _receivers.size(); ++i) {
    _receivers[i]->shutdown();
    delete _receivers[i];
  }
  _senders.clear();
  _receivers.clear();
  delete comm;
  // finally, shut down the receiver threads if any
  if (_dispatch_running) {
    _dispatch_running = false;
    for (size_t i = 0;i < _num_threads; ++i) {
      _thread_mutex[i].lock();
      _thread_cond[i].signal();
      _thread_mutex[i].unlock();
    }
    _thread_group.join(); 
  }
  mpi_tools::finalize();
}


void tcp_comm::send(int targetmachine, void* data, size_t length) {
 _senders[targetmachine]->copy_and_send_data(targetmachine,
                                            (char*)data, length);
}

void tcp_comm::send_relinquish(int targetmachine, void* data, size_t length) {
 _senders[targetmachine]->send_data2(targetmachine,
                                     (char*)data, length);
}



/** Receives a chunk of stuff */
void tcp_comm::chunk_receive(int machine, char* buf, size_t len) {
  // ok now I have a chunk of packets
  //allocate a chunk and insert it into the chunk queues
  chunk* c = new chunk;
  c->base = buf;
  c->cur = buf;
  c->len = len;
  c->remaining_len = len;
  c->refcount = 0;
  _recv_queue_lock[machine].lock();
  _recv_queue[machine].push_back(c);
  _recv_queue_lock[machine].unlock();
  // if we have receive threads running, we may need to wake one up
  if (_dispatch_running) {
    size_t target_thread = machine % _num_threads;
    // now, this is ok. Because if this flag is not set, then
    // we must be temporally ordered before the receiver thread's 2nd check
    // having this allows me to not take the lock every time.
    // Note that I still have to lock it to signal since I must be sure that
    // the thread is actually waiting in the condition variable
    if (_thread_trying_to_sleep[target_thread]) {
      _thread_mutex[target_thread].lock();
      _thread_cond[target_thread].signal();
      _thread_mutex[target_thread].unlock();
    }
  }
}

void tcp_comm::flush() {
  for (size_t i = 0;i < _senders.size(); ++i) {
    _senders[i]->flush();
  }
}

void tcp_comm::receiver_thread(size_t threadid) {

  _thread_mutex[threadid].lock(); 
  while(_dispatch_running) {
    // unlock the core mutex while I run stuff
    _thread_mutex[threadid].unlock(); 
    // get the queues I am in charge of and execute them
    for (size_t i = threadid; i < _size; i += _num_threads) {
      // fast exit
      if (_recv_queue[i].empty()) continue;
      std::deque<chunk*> myqueue;
      _recv_queue_lock[i].lock();
      _recv_queue[i].swap(myqueue);
      _recv_queue_lock[i].unlock();

      // now evaluate this queue
      // now since I have exclusive access to this queue
      // I don't need to worry about shared pointer and what not
      // more efficient to go by pointers directly
      while(!myqueue.empty()) {
        // get the current head
        chunk* curhead = myqueue.front();
        assert(curhead->remaining_len > 0);
        // try to read the chunk 
        while(1) {
          size_t length;
          void* retdata = advance_chunk(curhead, &length);
          if (retdata != NULL) {
            _receivefun(i, (char*)retdata, length);
          } else {
            break;
          }
        }
        myqueue.pop_front();
        delete curhead;
      }
    }
    // lock the core mutex while I do one more sweep to make sure there is 
    // nothing to do
    _thread_mutex[threadid].lock(); 
    _thread_trying_to_sleep[threadid] = true;
    // force a memory fence here
    asm volatile("mfence");
    bool allempty = true;
    for (size_t i = threadid; i < _size; i += _num_threads) {
      if (!_recv_queue[i].empty()) {
        allempty = false;
        break;
      }
    }
    // if there is really nothing to do, go to sleep
    if (allempty) _thread_cond[threadid].wait(_thread_mutex[threadid]);
    _thread_trying_to_sleep[threadid] = false;
    // force a memory fence here
    asm volatile("mfence");

  }
  _thread_mutex[threadid].unlock(); 
}

bool tcp_comm::register_receiver(const boost::function<void(int machine, const char* c, size_t len)>& receivefun,
                                 bool parallel) {
  if (_dispatch_running) return false;
  _receivefun = receivefun;
  if (parallel) {
    // how many threads to start? HEURISTIC
    // We start 1 every 2 machines, up to a maximum of 4
    _num_threads = std::min(4, _size / 2);
    // and at least 1
    if (_num_threads == 0) _num_threads = 1;
  }
  else {
    // only 1 thread possible
    _num_threads = 1;
  }

  _thread_mutex.resize(_num_threads);
  _thread_cond.resize(_num_threads);
  _thread_trying_to_sleep.resize(_num_threads, false);
  for (size_t i = 0;i < _num_threads ; ++i) {
    _thread_group.launch(boost::bind(&tcp_comm::receiver_thread, this, i));
  }
  _dispatch_running = true;
  // start the receive threads
  return true;
}


void tcp_comm::barrier() {
    MPI_Barrier(MPI_COMM_WORLD);
}

char* tcp_comm::advance_chunk(chunk* c, size_t* recvlen) {
  if (c->remaining_len == 0) {
    return NULL;
  }
  assert(c->remaining_len >= sizeof(dc_impl::packet_hdr));
  // read the header
  dc_impl::packet_hdr hdr = *reinterpret_cast<dc_impl::packet_hdr*>(c->cur);
  // the data is after the header
  char* ret = c->cur + sizeof(dc_impl::packet_hdr);
  // advance the pointers
  c->cur += sizeof(dc_impl::packet_hdr) + hdr.len;
  c->remaining_len -= (sizeof(dc_impl::packet_hdr) + hdr.len);
  assert(c->remaining_len < c->len);
  (*recvlen) = hdr.len;
  return ret;
}

void* tcp_comm::receive(int sourcemachine, size_t* length) {
  _recv_queue_lock[sourcemachine].lock();
  chunk* curhead;
  char* retdata = NULL;
  (*length) = 0;
  // if empty, unlock and return
  if (!_recv_queue[sourcemachine].empty()) {
    // get the current head
    curhead = _recv_queue[sourcemachine].front();
    // while we have this incremented, the head will always be around
    curhead->refcount.inc();
    assert(curhead->remaining_len > 0);
    // try to read the chunk 
    retdata = advance_chunk(curhead, length);
    // if there is no data remaining, pop it
    if (curhead->remaining_len == 0) {
      _recv_queue[sourcemachine].pop_front();
    }
  }
  _recv_queue_lock[sourcemachine].unlock();

  // now to return we need to make a copy
  if (retdata != NULL) {
    char* copy = (char*)malloc(*length);
    memcpy(copy, retdata, (*length));
    if (curhead->refcount.dec() == 0) delete curhead;

    return copy;
  } else {
    return NULL;
  }
}



void* tcp_comm::receive(int* sourcemachine, size_t* length) {
  size_t i = _last_receive_buffer_read_from + 1;
  // sweep from i,i+1... _size, 0, 1, 2, ... i-1
  // and try to receive from that buffer.
  // if all fail, we return NULL
  for (size_t j = 0; j < (size_t)_size; ++j) {
    void* ret = receive((j + i) % _size, length);
    if (ret != NULL) {
      (*sourcemachine) = (j + i) % _size;
      _last_receive_buffer_read_from = j;
      return ret;
    }
  }
  return NULL;
}

} // namespace graphlab
