#include <mpi.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <vector>
#include <boost/bind.hpp>
#include <graphlab/comm/mpi_comm2.hpp>
#include <graphlab/util/mpi_tools.hpp>
#include <graphlab/util/timer.hpp>
#include <graphlab/logger/logger.hpp>
namespace graphlab {

  
struct comm_header {
  uint64_t length;
};

mpi_comm2::mpi_comm2(int* argc, char*** argv, size_t send_window)
            :_send_window_size(send_window),_has_receiver(false) {
  // initializes mpi and record the rank and size
  int ret = mpi_tools::init(*argc, *argv, MPI_THREAD_MULTIPLE);
  bool has_mpi_thread_multiple = (ret == MPI_THREAD_MULTIPLE);

  _rank = mpi_tools::rank();
  _size = mpi_tools::size();

  if (_rank == 0 && !has_mpi_thread_multiple) {
    std::cerr << "We requested MPI to provided MPI_THREAD_MULTIPLE "
              << "multithreading support, but it can only provide level "
              << ret << ".\n"
              << "We will try to continue running, but this may not work.\n";
  }



  // create a new comm for this object
  MPI_Comm_dup(MPI_COMM_WORLD, &internal_comm);
  MPI_Comm_dup(MPI_COMM_WORLD, &external_comm);

  // reset counters
  _bytes_received = 0;
  _bytes_sent = 0;

  // --------- send buffer construction ---------
  for (size_t i = 0; i < 3; ++i) construct_window(i);
  _send_base[0] = _mmaped_buffers[0];
  _send_base[1] = _mmaped_buffers[1];
  _recv_base = _mmaped_buffers[2];

  // fill the length array. There is nothing to send to any machine.
  // so all lengths are 0
  _sendlength[0].assign(_size, 0);  _sendlength[1].assign(_size, 0);

  // fill the offset array. This is where the data destined to each machine
  // begins. Uniformly space the offsets across entire send window size
  _offset.resize(_size);
  _send_locks.resize(_size);
  _max_sendlength_per_machine = _send_window_size / _size;
  // round it to a multiple of the datatype size
  for (size_t i = 0; i < (size_t)_size; ++i) {
    _offset[i] = i * _max_sendlength_per_machine;
  }
  // now. if this is larger than max int, we need to scale it down
  if (_max_sendlength_per_machine > INT_MAX) _max_sendlength_per_machine = INT_MAX;

  // sends initially write to buffer 0
  _cur_send_buffer = 0; 
  // initialize the reference counters
  _buffer_reference_counts[0] = 0;
  _buffer_reference_counts[1] = 0;
  _last_garbage_collect_ms[0] = timer::approx_time_millis();
  _last_garbage_collect_ms[1] = timer::approx_time_millis();
  // ------------ receive buffer construction ---------
  // set the head sentinel value. essentially an empty buffer
  // values are not likely to matter anyway.
  _last_receive_buffer_read_from = 0;
  _receive_buffer.resize(_size);
  for (size_t i = 0;i < _receive_buffer.size(); ++i) {
    _receive_buffer[i].buflen = 0;
    _receive_buffer[i].next_message_length = 0;
  }
  _receive_thread_done = false;
  _send_thread_done = false;
  _send_thread.launch(boost::bind(&mpi_comm2::send_thread_function, this));
  _receive_thread.launch(boost::bind(&mpi_comm2::receive_thread_function, this));
}

void mpi_comm2::construct_window(size_t i) {
#ifdef __APPLE__
  _mmaped_buffers[i] = mmap(NULL, _send_window_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANON, -1, 0);
#else
  _mmaped_buffers[i] = mmap(NULL, _send_window_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
#endif
  if (_mmaped_buffers[i] == (void*)(-1)) {
    std::cerr << "Unable to mmap send window of size " << _send_window_size
              << " : " << errno << ". " << strerror(errno) << std::endl;
    assert(_mmaped_buffers[i] != NULL); 
  }
}

void mpi_comm2::destroy_window(size_t i) {
  munmap(_mmaped_buffers[i], _send_window_size);
  _send_base[i] = NULL;
}

void mpi_comm2::halt_threads() {
  // stop the communication threads
  // now... this is somewhat messy to do reliably
  // since MPI_Cancel may not actually unblock the receivers.
  //
  // The way we do this is to flush all the existing messages
  // Then ensure that all receive threads are waiting
  // Set the cancel flag
  // And issue 1 send to every machine
  
  // stop calling receive functions
  _has_receiver = false;

  // force flush the senders
  flush();
  barrier();

  // now we halt the send threads
  _flush_lock.lock();
  _send_thread_done = true;
  _flush_cond.signal();
  _flush_lock.unlock();
  _send_thread.join();

  barrier();

  // figure out how many bytes were sent in total
  size_t total_bytes_sent = 0; 
  size_t total_bytes_received = 0;
  MPI_Allreduce(&_bytes_sent, 
                &total_bytes_sent, 
                1, MPI_UNSIGNED_LONG, MPI_SUM, external_comm);
  if (rank() == 0) {
    std::cout << "MPI: " << total_bytes_sent << " total bytes sent\n";
  }

  // loop until all bytes have been received
  while(total_bytes_sent != total_bytes_received) {
    MPI_Allreduce(&_bytes_received,
                  &total_bytes_received, 
                  1, MPI_UNSIGNED_LONG, MPI_SUM, external_comm);
    graphlab::timer::sleep_ms(10);
  }
  
  // now we need to make sure that the receive thread is stuck in wait
  _receive_thread_lock.lock();
  barrier();

  // now we mark the receive thread cancellation
  _receive_thread_done = true;
  barrier();
  // and machine 0 sends 1 byte to every machine
  if (rank() == 0) {
    char nothing_of_importance = 0;
    for (int i = 0; i < size(); ++i) {
      MPI_Send(&nothing_of_importance, 1, MPI_CHAR, 
               i, 0 /* tag */, internal_comm);
    }
  }

  barrier();
  // now unlock so that the receivers can wake up
  _receive_thread_lock.unlock();

  _receive_thread.join();
  barrier();
}

mpi_comm2::~mpi_comm2() {
  halt_threads();
   
  MPI_Comm_free(&internal_comm);
  MPI_Comm_free(&external_comm);
  mpi_tools::finalize();
  // destroy all the windows
  for (size_t i = 0;i < 3; ++i) destroy_window(i);
}


void mpi_comm2::send(int targetmachine, void* _data, size_t length) {
  char* data = (char*)(_data);
//  printf("Sending %ld %d %d %d\n", length, (int)(data[0]), (int)(data[1]), int(data[2]));
  assert(0 <= targetmachine && targetmachine < _size);
  // 0 length messages not permitted
  assert(length > 0);
  // close loop around the actual send call which may split the
  // send up into multiple pieces
  // send a header
  comm_header hdr; hdr.length = length;
  size_t headerlength = sizeof(size_t); 
  char* headerptr = reinterpret_cast<char*>(&hdr);
  //bool printed_buf_full_message = false;
  
  _send_locks[targetmachine].lock();
  while (headerlength > 0) {
    size_t sent = 0;
    sent = actual_send(targetmachine, headerptr, headerlength);
    headerptr += sent; headerlength -= sent;
    // if we failed to write everything... we probably should try to flush
    if (headerlength > 0) {
   //   if (!printed_buf_full_message) std::cout << "buffer full\n";
   //   printed_buf_full_message = true;
      timer::sleep_ms(1);
    }
  }
  while (length > 0) {
    size_t sent = 0;
    sent = actual_send(targetmachine, data, length);
    data += sent; length -= sent;
    // if we failed to write everything... we probably should try to flush
    if (length > 0) {
   //   if (!printed_buf_full_message) std::cout << "buffer full\n";
   //   printed_buf_full_message = true;
      timer::sleep_ms(1);
    }
  }
  _send_locks[targetmachine].unlock();
} 


size_t mpi_comm2::actual_send(int targetmachine, void* data, size_t length) {
  // we need to pad to multiple of send_type

  // we first try to acquire the buffer. Usin the shared pointer to manage 
  // reference counts
  size_t target_buffer; // the send buffer ID
  size_t idx; // the buffer index to write to
  bool buffer_acquire_success = false;
  while (!buffer_acquire_success) {
    // get the buffer ID
    target_buffer = _cur_send_buffer.value;
    idx = target_buffer & 1;
    _buffer_reference_counts[idx].inc();
    // acquire a reference to it
    // if the buffer changed, we are in trouble. It means, we interleaved
    // with a flush. cancel it and try again
    buffer_acquire_success = (_cur_send_buffer.value == target_buffer);
    if (!buffer_acquire_success) {
      _buffer_reference_counts[idx].dec();
    }
  } 

  // Now, we got the buffer. Try to increment the length of the send as much
  // as we can. Then copy our buffer into the target buffer

  size_t oldsendlength;  // the original value of send length
  size_t maxwrite;   // the maximum amount I can write
  bool cas_success = false;
  while (!cas_success) {
    oldsendlength = _sendlength[idx][targetmachine].value;
    maxwrite = std::min(_max_sendlength_per_machine - oldsendlength, length);
    // if I cannot write anything, return
    if (maxwrite == 0) {
      _buffer_reference_counts[idx].dec();
      return 0;
    }
    cas_success = 
        (_sendlength[idx][targetmachine].value == oldsendlength) ? 
            _sendlength[idx][targetmachine].cas(oldsendlength, oldsendlength + maxwrite) 
            : false;
  }
  size_t written = std::min(maxwrite, length);
  memcpy((char*)(_send_base[idx]) + _offset[targetmachine] + oldsendlength,
         data,
         written);
  // The reference count will be automatically relinquished when we exit
  // if I wrote more than the actual data length, then it is with the padding.
  // in which case the write is done.
  
  _buffer_reference_counts[idx].dec();
  return written;
}


void mpi_comm2::flush() {
  _flush_lock.lock();
  _flush_cond.signal();
  _flush_lock.unlock();
}


size_t mpi_comm2::swap_buffers() {
  // increment the send buffer ID
  size_t idx = _cur_send_buffer.inc_ret_last() & 1;
  // wait for an exclusive lock on the buffer.
  // note that this is indeed a spin lock here.
  // the assumption is most sends are relatively short.
  // If this becomes an issue in the future, a condition variable solution
  // can be substituted with little work
  while(_buffer_reference_counts[idx] != 0) {
    cpu_relax();
  }
  return idx;
}

void mpi_comm2::receive_thread_function() {
  // AlltoAll scatter the buffer sizes
  _receive_thread_lock.lock();

  MPI_Request requests[_size];
  // post all the receives
  for (size_t i = 0;i < (size_t)_size; ++i) {
    MPI_Irecv((char*)_recv_base + _offset[i], 
              _max_sendlength_per_machine,
              MPI_CHAR,
              (int)i,
              MPI_ANY_TAG,
              internal_comm, requests + i);
  }
  
  while(1) {
    int completed_requests[_size];
    MPI_Status status[_size];
    int outcount; 
    _receive_thread_lock.unlock();
    MPI_Waitsome(_size, requests, &outcount, completed_requests, status);
    _receive_thread_lock.lock();
    if (_receive_thread_done) break;
    for (size_t i = 0; i < (size_t)outcount; ++i) {
      int bytes;
      MPI_Get_count(status + i, MPI_CHAR, &bytes);
      size_t machine = completed_requests[i];
      insert_receive_buffer(machine,
                            (char*)_recv_base + _offset[machine],
                            (size_t)bytes);
      // repost the wait
      MPI_Irecv((char*)_recv_base + _offset[machine], 
                _max_sendlength_per_machine,
                MPI_CHAR,
                (int)machine,
                MPI_ANY_TAG,
                internal_comm, requests + machine);

      // execute the callbacks
      if (_has_receiver) while(receiver_fun_receive() > 0);
    }
  }
  _receive_thread_lock.unlock();
}

void mpi_comm2::garbage_collect(size_t idx) {
  destroy_window(idx);
  construct_window(idx);
}


void mpi_comm2::reset_send_buffer(size_t idx) {
  for (size_t i = 0;i < (size_t)_size; ++i) {
    _sendlength[idx][i] = 0;
  }
  size_t curtime = timer::approx_time_millis();
  // more than 10 seconds since I last cleared
  if (curtime - _last_garbage_collect_ms[idx] > 10000) {
    garbage_collect(idx);
    _last_garbage_collect_ms[idx] = curtime;
  }
}

void mpi_comm2::locked_read_header_from_buffer(size_t idx) {
  receive_buffer_type& curbuf = _receive_buffer[idx];
  if (curbuf.next_message_length == 0 && curbuf.buflen >= sizeof(comm_header)) {
    comm_header header;
    curbuf.buffer.read(reinterpret_cast<char*>(&header), sizeof(comm_header));
    curbuf.next_message_length = header.length;
    curbuf.buflen -= sizeof(comm_header);
  }
}
void mpi_comm2::insert_receive_buffer(size_t idx, char* v, size_t length) {
  receive_buffer_type& curbuf = _receive_buffer[idx];
  curbuf.lock.lock();
  curbuf.buffer.write(v, length);
  _bytes_received += length;
  curbuf.buflen += length;
  locked_read_header_from_buffer(idx);
  curbuf.lock.unlock();
}

void* mpi_comm2::receive(int* sourcemachine, size_t* length) {
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
void* mpi_comm2::receive(int sourcemachine, size_t* length) {
  receive_buffer_type& curbuf = _receive_buffer[sourcemachine];
  // test for quick exit conditions which I don't have to lock
  if (curbuf.next_message_length == 0 ||
      curbuf.next_message_length > curbuf.buflen) return NULL;
  // ok. I have to lock
  void* ret = NULL;
  curbuf.lock.lock();
  if (curbuf.next_message_length > 0 &&  
      curbuf.next_message_length <= curbuf.buflen) {
    // ok we have enough to read the block
    ret = malloc(curbuf.next_message_length);
    assert(ret != NULL);   
    // read the buffer. and return
    curbuf.buffer.read((char*)ret, curbuf.next_message_length);
    curbuf.buflen -= curbuf.next_message_length;
    // if there is too much empty room in the buffer, we squeeze it
    // to conserve memory
    if ((size_t)curbuf.buffer.reserved_size() >= 5 * curbuf.buflen &&
        curbuf.buffer.reserved_size() > 4096) {
      curbuf.buffer.squeeze();
    }
    (*length) = curbuf.next_message_length;
    curbuf.next_message_length = 0;
    curbuf.next_message_length = 0;
    locked_read_header_from_buffer(sourcemachine);
  }
  curbuf.lock.unlock();
  return ret;
}


size_t mpi_comm2::receiver_fun_receive() {
  size_t i = _last_receive_buffer_read_from + 1;
  // sweep from i,i+1... _size, 0, 1, 2, ... i-1
  // and try to receive from that buffer.
  // if all fail, we return NULL
  for (size_t j = 0; j < (size_t)_size; ++j) {
    int count = receiver_fun_receive((j + i) % _size);
    if (count != 0) {
      return count;
    }
  }
  return 0;
}



size_t mpi_comm2::receiver_fun_receive(int sourcemachine) {
  receive_buffer_type& curbuf = _receive_buffer[sourcemachine];
  // test for quick exit conditions which I don't have to lock
  if (curbuf.next_message_length == 0 ||
      curbuf.next_message_length > curbuf.buflen) return 0;
  // ok. I have to lock
  curbuf.lock.lock();
  size_t numcalls = 0;
  while (curbuf.next_message_length > 0 &&  
      curbuf.next_message_length <= curbuf.buflen) {
    // ok we have enough to read the block
    char* ret = NULL;
    // can we read the array without an align? 
    if (curbuf.buffer.introspective_must_read(ret, curbuf.next_message_length) == false) {
      curbuf.buffer.align();
      bool success = curbuf.buffer.introspective_must_read(ret, curbuf.next_message_length);
      assert(success); 
    }
    curbuf.buflen -= curbuf.next_message_length;
    if (!_parallel_receiver) _receiver_lock.lock();
    _receivefun(sourcemachine, ret, curbuf.next_message_length);
    if (!_parallel_receiver) _receiver_lock.unlock();
    ++numcalls;
    // if there is too much empty room in the buffer, we squeeze it
    // to conserve memory
    if ((size_t)curbuf.buffer.reserved_size() >= 5 * curbuf.buflen &&
        curbuf.buffer.reserved_size() > 4096) {
      curbuf.buffer.squeeze();
    }
    curbuf.next_message_length = 0;
    locked_read_header_from_buffer(sourcemachine);
  }
  curbuf.lock.unlock();
  return numcalls;
} 

bool mpi_comm2::register_receiver(
    const boost::function<void(int machine, const char* c, size_t len)>& receivefun,
    bool parallel) {
  if (_has_receiver == false) {
    // lock the all flush operations before setting the receiver
    _receive_thread_lock.lock();
    _receivefun = receivefun; 
    _has_receiver = true;
    _parallel_receiver = parallel;
    _receive_thread_lock.unlock();
    return true;
  } else {
    return false;
  }
}



void mpi_comm2::send_thread_function() {
  _flush_lock.lock();
  while (!_send_thread_done) {
    _flush_cond.timedwait_ms(_flush_lock, 10);
    size_t idx = swap_buffers();
    // ok loop through the buffers and send anything that is non-empty
    for (size_t i = 0;i < _size; ++i) {
      if (_sendlength[idx][i] > 0) {
        _bytes_sent += _sendlength[idx][i];
        MPI_Send((char*)_send_base[idx] + _offset[i], 
                 _sendlength[idx][i],
                 MPI_CHAR, i, 
                 0 /* tag */, internal_comm);
        _sendlength[idx][i] = 0;
      }
    }
  } 
  _flush_lock.unlock();
}


void mpi_comm2::barrier() {
  MPI_Barrier(external_comm);
}


void mpi_comm2::send_relinquish(int targetmachine, void* data, size_t length) {
  send(targetmachine, data, length);
  free(data);
}

} // namespace graphlab
