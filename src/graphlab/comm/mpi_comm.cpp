#include <mpi.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <vector>
#include <boost/bind.hpp>
#include <graphlab/comm/mpi_comm.hpp>
#include <graphlab/util/mpi_tools.hpp>
#include <graphlab/util/timer.hpp>
#include <graphlab/logger/logger.hpp>
namespace graphlab {

  
static const MPI_Datatype MPI_SEND_TYPE = MPI_LONG;
typedef uint64_t send_type; 


struct comm_header {
  uint64_t length;
};

size_t get_padded_length(size_t length) {
  size_t paddedlength = ((length / sizeof(send_type)) + 
                         (length % sizeof(send_type) > 0)) * sizeof(send_type);
  return paddedlength;
}

mpi_comm::mpi_comm(int* argc, char*** argv, size_t send_window)
            :_send_window_size(send_window),_has_receiver(false) {
  // initializes mpi and record the rank and size
  int ret = mpi_tools::init(*argc, *argv, MPI_THREAD_MULTIPLE);
  _has_mpi_thread_multiple = (ret == MPI_THREAD_MULTIPLE);
    _local_barrier_count = 0; 
  _global_barrier_count = 0;

  _rank = mpi_tools::rank();
  _size = mpi_tools::size();

  if (_rank == 0 && !_has_mpi_thread_multiple) {
    std::cerr << "We requested MPI to provided MPI_THREAD_MULTIPLE "
              << "multithreading support, but it can only provide level "
              << ret << ".\n"
              << "The system will still continue to operate, but performance "
              << "will be degraded.\n";
  }



  // create a new comm for this object
  MPI_Comm_dup(MPI_COMM_WORLD, &internal_comm);
  MPI_Comm_dup(MPI_COMM_WORLD, &external_comm);

  // --------- send buffer construction ---------
  construct_send_window(0); construct_send_window(1);
  // fill the length array. There is nothing to send to any machine.
  // so all lengths are 0
  _sendlength[0].assign(_size, 0);  _sendlength[1].assign(_size, 0);

  // fill the offset array. This is where the data destined to each machine
  // begins. Uniformly space the offsets across entire send window size
  _offset.resize(_size); _offset_by_datatype.resize(_size);
  _send_locks.resize(_size);
  _max_sendlength_per_machine = _send_window_size / _size;
  // round it to a multiple of the datatype size
  _max_sendlength_per_machine  = 
        (_max_sendlength_per_machine / sizeof(send_type)) * sizeof(send_type);
  for (size_t i = 0; i < (size_t)_size; ++i) {
    _offset[i] = i * _max_sendlength_per_machine;
    _offset_by_datatype[i] = _offset[i] / sizeof(send_type);
  }

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
    _receive_buffer[i].padded_next_message_length = 0;
  }
  _num_nodes_flushing_threads_done = 0;
  _flushing_thread_done = false;
  _flushing_thread.launch(boost::bind(&mpi_comm::background_flush, this));
}

void mpi_comm::construct_send_window(size_t i) {
#ifdef __APPLE__
  _send_base[i] = mmap(NULL, _send_window_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANON, -1, 0);
#else
  _send_base[i] = mmap(NULL, _send_window_size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
#endif
  if (_send_base[i] == (void*)(-1)) {
    std::cerr << "Unable to mmap send window of size " << _send_window_size
              << " : " << errno << ". " << strerror(errno) << std::endl;
    assert(_send_base[i] != NULL); 
  }
}

void mpi_comm::destroy_send_window(size_t i) {
  munmap(_send_base[i], _send_window_size);
  _send_base[i] = NULL;
}

mpi_comm::~mpi_comm() {
  // stop the flushing thread
  _flushing_thread_done = true;
  _flushing_thread.join();
 
  MPI_Comm_free(&internal_comm);
  MPI_Comm_free(&external_comm);
  mpi_tools::finalize();
}

mutex send_lock;

void mpi_comm::send(int targetmachine, void* _data, size_t length) {
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


size_t mpi_comm::actual_send(int targetmachine, void* data, size_t length) {
  // we need to pad to multiple of send_type
  size_t paddedlength = get_padded_length(length);

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
    maxwrite = std::min(_max_sendlength_per_machine - oldsendlength, paddedlength);
    assert(maxwrite <= paddedlength);
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


void mpi_comm::flush() {
  // the proper way to flush will be to invoke the background flush inner op.
  // but this does not play nice with MPI implementations that do not have
  // MPI_THREAD_MULTIPLE support
  if (_has_mpi_thread_multiple) {
    background_flush_inner_op();
  } else {
    // here is therefore an alternate implementation that is quite a 
    // bit uglier
    size_t idx = _cur_send_buffer.value;
    // looping until the value increments by 2 is a sure way
    // to ensure that the flush has completed.
    while(_cur_send_buffer.value - idx < 2) {
      timer::sleep_ms(5);
    }
  }
}


size_t mpi_comm::swap_buffers() {
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

void mpi_comm::actual_flush(size_t idx, MPI_Comm communicator) {
  // AlltoAll scatter the buffer sizes
  int send_buffer_sizes[_size];

  for (size_t i = 0;i < (size_t)_size; ++i) {
    // the size of the send buffer in send_type steps
    assert(_sendlength[idx][i] % sizeof(send_type) == 0);
    send_buffer_sizes[i] = _sendlength[idx][i] / sizeof(send_type);
  }


  int recv_buffer_offsets[_size];
  int recv_buffer_sizes[_size];
  recv_buffer_sizes[0] = 0;
  int error = MPI_Alltoall(&(send_buffer_sizes[0]),
                           1,
                           MPI_INT,
                           &(recv_buffer_sizes[0]),
                           1,
                           MPI_INT,
                           communicator);
  ASSERT_EQ(error, MPI_SUCCESS);

  // now allocate the receive buffers
  size_t total_receive = 0;
  for (size_t i = 0;i < (size_t)_size; ++i) {
    recv_buffer_offsets[i] = total_receive;
    total_receive += recv_buffer_sizes[i];
  }
  void* receiveptr = malloc(total_receive * sizeof(send_type));
  error = MPI_Alltoallv(_send_base[idx], 
                        &(send_buffer_sizes[0]),
                        &(_offset_by_datatype[0]),
                        MPI_SEND_TYPE,
                        receiveptr,
                        &(recv_buffer_sizes[0]),
                        &(recv_buffer_offsets[0]),
                        MPI_SEND_TYPE,
                        communicator);
  ASSERT_EQ(error, MPI_SUCCESS);
  // cut up the receive buffer into little pieces for each receiving buffer
  for (size_t i = 0;i < (size_t)_size; ++i) {
    if (recv_buffer_sizes[i] > 0) {
      char* recvptr = ((char*)(receiveptr)) + 
          recv_buffer_offsets[i] * sizeof(send_type);
      insert_receive_buffer(i, 
                            recvptr, 
                            recv_buffer_sizes[i] * sizeof(send_type));
    }
  }
  free(receiveptr);

  // reset the current buffer so it can be reused 
  reset_send_buffer(idx);
}

void mpi_comm::garbage_collect(size_t idx) {
  destroy_send_window(idx);
  construct_send_window(idx);
}


void mpi_comm::reset_send_buffer(size_t idx) {
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

void mpi_comm::locked_read_header_from_buffer(size_t idx) {
  receive_buffer_type& curbuf = _receive_buffer[idx];
  if (curbuf.next_message_length == 0 && curbuf.buflen >= sizeof(comm_header)) {
    comm_header header;
    curbuf.buffer.read(reinterpret_cast<char*>(&header), sizeof(comm_header));
    curbuf.next_message_length = header.length;
    curbuf.padded_next_message_length = get_padded_length(header.length);
    curbuf.buflen -= sizeof(comm_header);
  }
}
void mpi_comm::insert_receive_buffer(size_t idx, char* v, size_t length) {
  receive_buffer_type& curbuf = _receive_buffer[idx];
  curbuf.lock.lock();
  curbuf.buffer.write(v, length);
  curbuf.buflen += length;
  locked_read_header_from_buffer(idx);
  curbuf.lock.unlock();
}

void* mpi_comm::receive(int* sourcemachine, size_t* length) {
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
void* mpi_comm::receive(int sourcemachine, size_t* length) {
  receive_buffer_type& curbuf = _receive_buffer[sourcemachine];
  // test for quick exit conditions which I don't have to lock
  if (curbuf.padded_next_message_length == 0 ||
      curbuf.padded_next_message_length > curbuf.buflen) return NULL;
  // ok. I have to lock
  void* ret = NULL;
  curbuf.lock.lock();
  if (curbuf.padded_next_message_length > 0 &&  
      curbuf.padded_next_message_length <= curbuf.buflen) {
    // ok we have enough to read the block
    ret = malloc(curbuf.padded_next_message_length);
    assert(ret != NULL);   
    // read the buffer. and return
    curbuf.buffer.read((char*)ret, curbuf.padded_next_message_length);
    curbuf.buflen -= curbuf.padded_next_message_length;
    // if there is too much empty room in the buffer, we squeeze it
    // to conserve memory
    if ((size_t)curbuf.buffer.reserved_size() >= 5 * curbuf.buflen &&
        curbuf.buffer.reserved_size() > 4096) {
      curbuf.buffer.squeeze();
    }
    (*length) = curbuf.next_message_length;
    curbuf.next_message_length = 0;
    curbuf.padded_next_message_length = 0;
    locked_read_header_from_buffer(sourcemachine);
  }
  curbuf.lock.unlock();
  return ret;
}


size_t mpi_comm::receiver_fun_receive() {
  assert(_has_receiver);
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



size_t mpi_comm::receiver_fun_receive(int sourcemachine) {
  assert(_has_receiver);
  receive_buffer_type& curbuf = _receive_buffer[sourcemachine];
  // test for quick exit conditions which I don't have to lock
  if (curbuf.padded_next_message_length == 0 ||
      curbuf.padded_next_message_length > curbuf.buflen) return 0;
  // ok. I have to lock
  curbuf.lock.lock();
  size_t numcalls = 0;
  while (curbuf.padded_next_message_length > 0 &&  
      curbuf.padded_next_message_length <= curbuf.buflen) {
    // ok we have enough to read the block
    char* ret = NULL;
    // can we read the array without an align? 
    if (curbuf.buffer.introspective_must_read(ret, curbuf.padded_next_message_length) == false) {
      curbuf.buffer.align();
      bool success = curbuf.buffer.introspective_must_read(ret, curbuf.padded_next_message_length);
      assert(success); 
    }
    curbuf.buflen -= curbuf.padded_next_message_length;
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
    curbuf.padded_next_message_length = 0;
    locked_read_header_from_buffer(sourcemachine);
  }
  curbuf.lock.unlock();
  return numcalls;
} 

bool mpi_comm::register_receiver(
    const boost::function<void(int machine, const char* c, size_t len)>& receivefun,
    bool parallel) {
  if (_has_receiver == false) {
    // lock the all flush operations before setting the receiver
    _background_flush_inner_op_lock.lock();
    _flush_lock.lock();
    _receivefun = receivefun; 
    _has_receiver = true;
    _parallel_receiver = parallel;
    _flush_lock.unlock();
    _background_flush_inner_op_lock.unlock();
    return true;
  } else {
    return false;
  }
}



void mpi_comm::background_flush_inner_op() {
  _background_flush_inner_op_lock.lock();
  // this ensures that once the flushing threads quit,
  // we don't start any more flushing MPI operations
  // since these will not have a matching call on the other nodes.
  if (_num_nodes_flushing_threads_done < _size) {
    _flush_lock.lock();
    size_t idx = swap_buffers();
    actual_flush(idx, internal_comm);
    _flush_lock.unlock();

    int send = _flushing_thread_done;
    MPI_Allreduce(&send, 
                  &_num_nodes_flushing_threads_done, 
                  1, MPI_INT, MPI_SUM, internal_comm);

    if (!_has_mpi_thread_multiple) {
      // we have to simulate the barrier here
      MPI_Allreduce(&_local_barrier_count, 
                  &_global_barrier_count, 
                  1, MPI_INT, MPI_MIN, internal_comm);
    }
  }
  _background_flush_inner_op_lock.unlock();
  if (_has_receiver) while(receiver_fun_receive() > 0);
}

void mpi_comm::background_flush() {
  while (_num_nodes_flushing_threads_done < _size) {
    timer::sleep_ms(10);
    background_flush_inner_op();
  } 
}


void mpi_comm::barrier() {
  if (_has_mpi_thread_multiple) {
    MPI_Barrier(external_comm);
  } else {
    ++_local_barrier_count;
    while (_global_barrier_count < _local_barrier_count) {
      timer::sleep_ms(5);
    }
  }
}


void mpi_comm::send_relinquish(int targetmachine, void* data, size_t length) {
  send(targetmachine, data, length);
  free(data);
}

} // namespace graphlab
