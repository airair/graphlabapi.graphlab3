/*  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#ifndef DC_BUFFERED_STREAM_SEND2_HPP
#define DC_BUFFERED_STREAM_SEND2_HPP
#include <iostream>
#include <boost/function.hpp>
#include <graphlab/parallel/atomic.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
#include <graphlab/util/resizing_array_sink.hpp>
#include <graphlab/comm/tcp/circular_iovec_buffer.hpp>
#include <graphlab/logger/logger.hpp>
namespace graphlab {

namespace dc_impl {

class dc_tcp_comm;

/**
 * \internal
   \ingroup rpc
Sender for the dc class.
  The job of the sender is to take as input data blocks of
  pieces which should be sent to a single destination socket.
  This can be thought of as a sending end of a multiplexor.
  This class performs buffered transmissions using an blocking 
  queue with one call per queue entry.
  A seperate thread is used to transmit queue entries. Rudimentary
  write combining is used to decrease transmission overhead.
  This is typically the best performing sender.
  
  This can be enabled by passing "buffered_queued_send=yes"
  in the distributed control initstring.
  
  dc_buffered_stream_send22 is similar, but does not perform write combining.
  
*/

class dc_buffered_stream_send2{
 public:
  dc_buffered_stream_send2(dc_tcp_comm* comm, 
                           int procid,
                           int target);

  inline ~dc_buffered_stream_send2() { }
                 

  /** Called to send data to the target. The caller transfers control of
  the pointer. The caller MUST ensure that the data be prefixed
  with sizeof(packet_hdr) extra bytes at the start for placement of the
  packet header. */
  void send_data(int target, char* data, size_t len);


/** Called to send data to the target. The caller transfers control of
  the pointer.  */
  void send_data2(int target, char* data, size_t len);


  /** Sends the data but without transferring control of the pointer.
   The function will make a copy of the data before sending it.
   */
  void copy_and_send_data(int target,
                          char* data, size_t len);

  size_t get_outgoing_data(circular_iovec_buffer& outdata);
  
  
  inline size_t bytes_sent() {
    return bytessent.value;
  }

  size_t send_queue_length() const {
    return writebuffer_totallen.value;
  }
  
  void flush();

 private:
  /// pointer to the owner
  dc_tcp_comm* comm;
  int procid;
  int target;

  atomic<size_t> writebuffer_totallen;
  
  struct buffer_and_refcount{
    std::vector<iovec> buf;
    atomic<size_t> numel;
    atomic<size_t> numbytes;
    volatile int32_t ref_count; // if negative, means it is sending
  };
  buffer_and_refcount buffer[2];
  size_t bufid;
  

  atomic<size_t> bytessent; 
  

};



} // namespace dc_impl
} // namespace graphlab
#endif // DC_BUFFERED_STREAM_SEND_EXPQUEUE_HPP

