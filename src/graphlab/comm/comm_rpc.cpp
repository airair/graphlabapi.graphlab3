#include <cassert>
#include <boost/bind.hpp>
#include <graphlab/comm/comm_base.hpp>
#include <graphlab/comm/comm_rpc.hpp>
namespace graphlab {



comm_rpc::comm_rpc(comm_base* comm):
    _comm(comm),_dispatch_table(65536) {
  _pool.reset_pool(64);
  bool ret = comm->register_receiver(
      boost::bind(&comm_rpc::receiver, this, _1, _2, _3), 
      true);
  _comm_has_efficient_send = comm->has_efficient_send();
  assert(ret);
}


comm_rpc::~comm_rpc() {
  _dispatch_table.clear();
  // free the contents of the pool
  std::vector<oarchive>& arcref = _pool.unsafe_get_pool_ref();
  for (size_t i = 0;i < arcref.size(); ++i) {
    if (arcref[i].buf != NULL) free(arcref[i].buf);
  }
}


void comm_rpc::receiver(int machine, const char* c, size_t len) {
  assert(len >= 2);
  unsigned short message = *reinterpret_cast<const unsigned short*>(c);
  assert(_dispatch_table[message] != NULL); 
  _dispatch_table[message](this, machine, c + 2, len - 2);
}

void comm_rpc::register_handler(unsigned short message_id,
                                const dispatch_function_type& function) {
  assert(_dispatch_table[message_id] == NULL);
  _dispatch_table[message_id] = function;
}

void comm_rpc::send_message(int machine, 
                            unsigned short message_id, 
                            const char* data, size_t len) {
  char* newdata = (char*)malloc(len + sizeof(unsigned short));  
  memcpy(newdata + 2, data, len);
  (*reinterpret_cast<unsigned short*>(newdata)) = message_id;
  _comm->send(machine, newdata, len + 2);
  free(newdata);
}

graphlab::oarchive* comm_rpc::prepare_message(unsigned short message_id) {
  graphlab::oarchive* arc = _pool.alloc();
  assert(arc->off == 0);
  (*arc) << message_id;
  return arc;
}

void comm_rpc::complete_message(int machine, graphlab::oarchive* arc) {
  if (_comm_has_efficient_send) {
    // send is efficient. We maintain the buffer and do not give it up
    // to the comm
    _comm->send(machine, arc->buf, arc->off);
    // reset the offset so we can reuse this buffer
    arc->off = 0;
    // test if the arc is part of the pool
    if (_pool.is_pool_member(arc) == false) {
      free(arc->buf);
      arc->buf = NULL;
    }
    _pool.free(arc);
  } else {
    // send_relinquish is more efficient. We give up the buffer
    _comm->send_relinquish(machine, arc->buf, arc->off);
    arc->buf = NULL;
    arc->off = 0;
    arc->len = 0;
    _pool.free(arc);
  }
}


} // namespace graphlab
