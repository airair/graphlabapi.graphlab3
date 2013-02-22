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


#ifndef DC_STREAM_RECEIVE_HPP
#define DC_STREAM_RECEIVE_HPP
#include <boost/function.hpp>
#include <graphlab/util/circular_char_buffer.hpp>
#include <graphlab/parallel/atomic.hpp>
#include <graphlab/parallel/pthread_tools.hpp>
#include <graphlab/logger/logger.hpp>
#include <graphlab/comm/tcp/packet_header.hpp>
namespace graphlab {

namespace dc_impl {

/**
 * \internal
  \ingroup rpc
  Receiver processor for the dc class.
  The job of the receiver is to take as input a byte stream
  (as received from the socket) and cut it up into meaningful chunks.
  This can be thought of as a receiving end of a multiplexor.
  
  This is the default unbuffered receiver.
*/
class dc_stream_receive{
 public:
  typedef boost::function<void(int associated_proc, char* buf, size_t len)> receiver_function_type;

  dc_stream_receive(const receiver_function_type& receiver_function, int associated_proc): 
                  receiver_function(receiver_function),
                  header_read(0), writebuffer(NULL), 
                  write_buffer_written(0), associated_proc(associated_proc)
                   { }
  
  void shutdown();

  char* get_buffer(size_t& retbuflength);
  

  char* advance_buffer(char* c, size_t wrotelength, 
                              size_t& retbuflength);

 private:

  receiver_function_type receiver_function;
  size_t header_read;
  block_header_type cur_chunk_header;
  char* writebuffer;
  size_t write_buffer_written;
  
  int associated_proc;
  

  
    
};


} // namespace dc_impl
} // namespace graphlab
#endif

