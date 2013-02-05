#ifndef GRAPHLAB_COMM_TCP_PACKET_HEADER_HPP
#define GRAPHLAB_COMM_TCP_PACKET_HEADER_HPP

namespace graphlab{
namespace dc_impl {

struct packet_hdr{
  uint32_t len;
  int src;
};


typedef uint32_t block_header_type;

} }
#endif
