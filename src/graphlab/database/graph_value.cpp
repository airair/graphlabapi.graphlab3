#include <graphlab/database/graph_value.hpp>
#include <cstring>
#include <cstdlib>
namespace graphlab {

graph_value::graph_value(): 
    _len(sizeof(graph_int_t)), 
    _type(INT_TYPE), 
    _null_value(true),
    _modified(false),
    _use_delta_commit(false) {
      memset(&_data, 0, sizeof(_data));
      memset(&_old, 0, sizeof(_old));
    }

graph_value::~graph_value() {
  free_data();
}


void graph_value::free_data() {
  if ((_type == STRING_TYPE || _type == BLOB_TYPE) &&
      _data.bytes != NULL) {
    free(_data.bytes);
    _data.bytes = NULL;
    _len = 0;
  }
}

const char* graph_value::get_raw_pointer() {
  if (is_null()) {
    return NULL;
  } else if (is_scalar_graph_datatype(_type)) {
    return reinterpret_cast<char*>(&_data);
  } else {
    return _data.bytes;
  }
}

char* graph_value::get_mutable_raw_pointer() {
  if (is_null()) {
    return NULL;
  } else if (is_scalar_graph_datatype(_type)) {
    _modified = true;
    return reinterpret_cast<char*>(&_data);
  } else {
    _modified = true;
    return _data.bytes;
  }
}



bool graph_value::get_vid(graph_vid_t* out_ret) {
  if (type() == VID_TYPE && !is_null()) {
    (*out_ret) = _data.int_value;
    return true;
  } else { 
    return false;
  }
} 

bool graph_value::get_integer(graph_int_t* out_ret) {
  if (type() == INT_TYPE && !is_null()) {
    (*out_ret) = _data.vid_value;
    return true;
  } else { 
    return false;
  }
} 

bool graph_value::get_double(graph_double_t* out_ret) {
  if (type() == DOUBLE_TYPE && !is_null()) {
    (*out_ret) = _data.double_value;
    return true;
  } else { 
    return false;
  }
} 


bool graph_value::get_string(graph_string_t* out_ret) {
  if (type() == STRING_TYPE && !is_null()) {
    out_ret->assign(_data.bytes, _len);
    return true;
  } else { 
    return false;
  }
} 

bool graph_value::get_blob(graph_blob_t* out_ret) {
  if (type() == BLOB_TYPE && !is_null()) {
    out_ret->assign(_data.bytes, _len);
    return true;
  } else { 
    return false;
  }
} 


bool graph_value::get_blob(size_t len, char* out_blob) {
  if (type() == BLOB_TYPE && !is_null()) {
    memcpy(out_blob, _data.bytes, std::min(len, data_length()));
    return true;
  } else {
    return false;
  }
}

bool graph_value::set_integer(graph_int_t val) {
  if (type() == INT_TYPE) {
    // if the modified flag was already set, leave it.
    // otherwise, set it only if the value changed.
    _modified = (_modified || _data.int_value != val);
    _data.int_value = val;
    _null_value = false;
    return true;
  } else {
    return false;
  }
}

bool graph_value::set_double(graph_double_t val) {
  if (type() == DOUBLE_TYPE) {
    // if the modified flag was already set, leave it.
    // otherwise, set it only if the value changed.
    _modified = (_modified || _data.int_value != val);
    _data.double_value = val;
    _null_value = false;
    return true;
  } else {
    return false;
  }
}


bool graph_value::set_string(const graph_string_t& val){
  if (type() == STRING_TYPE) {
    // if we need to resize.
    _null_value = false;
    if (val.length() != _len) {
      _len = val.length();
      _data.bytes = reinterpret_cast<char*>(realloc(_data.bytes, _len));
      memcpy(_data.bytes, val.c_str(), _len);
      _modified = true;
    }
    else if (memcmp(_data.bytes, val.c_str(), _len) != 0) {
      _modified = true;
      memcpy(_data.bytes, val.c_str(), _len);
    }
    return true;
  } else {
    return false;
  }
}

bool graph_value::set_blob(const graph_blob_t& val){
  return set_blob(val.c_str(), val.length());
}

bool graph_value::set_blob(const char* val, size_t length) {
  if (type() == BLOB_TYPE) {
    // if we need to resize.
    _null_value = false;
    if (length != _len) {
      _len = length;
      _data.bytes = reinterpret_cast<char*>(realloc(_data.bytes, _len));
      memcpy(_data.bytes, val, _len);
      _modified = true;
    }
    else if (val == _data.bytes) {
      _modified = true;
    }
    else if (memcmp(_data.bytes, val, _len) != 0) {
      memcpy(_data.bytes, val, _len);
      _modified = true;
    }
    return true;
  } else {
    return false;
  }
}

} // namespace graphlab
