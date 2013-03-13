#include <graphlab/database/graph_value.hpp>
#include <cstring>
#include <cstdlib>
namespace graphlab {
  graph_value::graph_value(): 
      _len(sizeof(graph_int_t)), 
      _type(INT_TYPE), 
      _null_value(true) {
        memset(&_data, 0, sizeof(_data));
      }

  graph_value::graph_value(graph_datatypes_enum type) {
    init(type);
  }

  graph_value::graph_value(const graph_value& other) :
      _len(other._len), _type(other._type), _null_value(other._null_value) {
        if (is_scalar_graph_datatype(_type)) {
          _data = other._data;
        } else {
          _data.bytes = (char*) malloc(_len);
          memcpy(_data.bytes, other._data.bytes, _len);
        }
      }

  graph_value& graph_value::operator=(const graph_value& other) { 
    _type = other._type;
    _len = other._len;
    _null_value = other._null_value;
    if (is_scalar_graph_datatype(_type)) {
      _data = other._data;
    } else {
      if(_data.bytes != NULL) {
        _data.bytes = (char*) realloc(_data.bytes, _len);
      } else {
        _data.bytes = (char*) malloc(_len);
      }
      memcpy(_data.bytes, other._data.bytes, _len);
    }
    return *this; 
  }


  void graph_value::init(graph_datatypes_enum type) {
    _type = type; 
    _null_value = true;
    memset(&_data, 0, sizeof(_data));
    switch(type) {
     case STRING_TYPE:
     case BLOB_TYPE: _len = 0;  break;
     case DOUBLE_TYPE: _len = sizeof(graph_double_t); break;
     case INT_TYPE: _len =sizeof(graph_int_t); break;
     case VID_TYPE: _len =sizeof(graph_vid_t); break;
     default: ASSERT_TRUE(false); // this should never happen;
    }
  }

  graph_value::~graph_value() {
    free_data();
  }

  void graph_value::free_data() {
    if ((_type == STRING_TYPE || _type == BLOB_TYPE)) { 
      if (_data.bytes != NULL) {
        free(_data.bytes);
        _data.bytes = NULL;
        _len = 0;
      }
    }
  }

  const void* graph_value::get_raw_pointer() const {
    if (is_null()) {
      return NULL;
    } else if (is_scalar_graph_datatype(_type)) {
      return reinterpret_cast<const void*>(&_data);
    } else {
      return reinterpret_cast<const void*>(_data.bytes);
    }
  }

  void* graph_value::get_mutable_raw_pointer() {
    if (is_null()) {
      return NULL;
    } else if (is_scalar_graph_datatype(_type)) {
      return reinterpret_cast<void*>(&_data);
    } else {
      return reinterpret_cast<void*>(_data.bytes);
    }
  }

  bool graph_value::get_vid(graph_vid_t* out_ret) const {
    if (type() == VID_TYPE && !is_null()) {
      (*out_ret) = _data.int_value;
      return true;
    } else { 
      return false;
    }
  } 

  bool graph_value::get_integer(graph_int_t* out_ret) const {
    if (type() == INT_TYPE && !is_null()) {
      (*out_ret) = _data.vid_value;
      return true;
    } else { 
      return false;
    }
  } 

  bool graph_value::get_double(graph_double_t* out_ret) const {
    if (type() == DOUBLE_TYPE && !is_null()) {
      (*out_ret) = _data.double_value;
      return true;
    } else { 
      return false;
    }
  } 

  bool graph_value::get_string(graph_string_t* out_ret) const {
    if (type() == STRING_TYPE && !is_null()) {
      out_ret->assign(_data.bytes, _len);
      return true;
    } else { 
      return false;
    }
  } 

  bool graph_value::get_blob(graph_blob_t* out_ret) const {
    if (type() == BLOB_TYPE && !is_null()) {
      out_ret->assign(_data.bytes, _len);
      return true;
    } else { 
      return false;
    }
  } 

  bool graph_value::get_blob(size_t len, char* out_blob) const {
    if (type() == BLOB_TYPE && !is_null()) {
      memcpy(out_blob, _data.bytes, std::min(len, data_length()));
      return true;
    } else {
      return false;
    }
  }

  bool graph_value::set_val(const graph_value& other, bool delta) {
    if (_type != other._type) {
      return false;
    }
    const char* data_ptr =  is_scalar_graph_datatype(_type) ? (const char*)(&other._data) : 
        (const char*)(other._data.bytes);
    return set_val(data_ptr, other._len, delta);
  }

  bool graph_value::set_val(const char* val, size_t length, bool delta) {
    switch(_type) {
     case INT_TYPE:
       {
         graph_int_t intval = *((graph_int_t*)val);
         return set_integer(intval, delta); 
       }
     case DOUBLE_TYPE:
       {
         graph_double_t doubleval = *((graph_double_t*)val);
         return set_double(doubleval, delta); 
       }
     case VID_TYPE:
       return set_vid(*((graph_vid_t*)val));
     case STRING_TYPE:
       return set_string(std::string(val, length));
     case BLOB_TYPE:
       return set_blob(val, length);
     default:
       return false;
    }
    return false;
  }

  bool graph_value::set_val(const std::string& val_str, bool delta) {
    switch(_type) {
     case INT_TYPE:
       {
         try {
           graph_int_t intval = boost::lexical_cast<graph_int_t>(val_str); 
           return set_integer(intval, delta);
         } catch (boost::bad_lexical_cast &) {
           logstream(LOG_ERROR) << "Unable to cast "
                                << val_str << " to graph_int_t" << std::endl;
           return false;
         }
       }
     case DOUBLE_TYPE:
       {
         try {
           graph_double_t doubleval = boost::lexical_cast<graph_double_t>(val_str);
           return set_double(doubleval, delta);
         } catch (boost::bad_lexical_cast &) {
           logstream(LOG_ERROR) << "Unablt ot cast "
                                << val_str << " to graph_dobuble_t" << std::endl;
         }
       }
     case VID_TYPE:
       return set_vid(boost::lexical_cast<graph_vid_t>(val_str));
     case STRING_TYPE:
       return set_string(val_str);
     case BLOB_TYPE:
       return set_blob(val_str.c_str(), val_str.size());
     default:
       return false;
    }
    return false;
  }

  bool graph_value::set_integer(graph_int_t val, bool is_delta) {
    if (type() == INT_TYPE) {
      if (is_delta) {
        _data.int_value += val;
      } else {
        _data.int_value = val;
      }
      _null_value = false;
      return true;
    } else {
      return false;
    }
  }

  bool graph_value::set_double(graph_double_t val, bool is_delta) {
    if (type() == DOUBLE_TYPE) {
      if (is_delta) {
        _data.double_value += val;
      } else {
        _data.double_value = val;
      }
      _null_value = false;
      return true;
    } else {
      return false;
    }
  }

  bool graph_value::set_vid(graph_vid_t val) {
    if (type() == VID_TYPE) {
      _data.vid_value = val;
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
      }
      else if (memcmp(_data.bytes, val.c_str(), _len) != 0) {
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
      } else {
        if (val != _data.bytes) 
          memcpy(_data.bytes, val, _len);
      } 
      return true;
    } else {
      return false;
    }
  }

  void graph_value::diff(const graph_value& other, graph_value& out_delta) {
    out_delta = *this;
    if (is_scalar_graph_datatype(_type) && (_type == other._type)) {
      switch (_type) {
       case INT_TYPE: out_delta._data.int_value -= other._data.int_value; break;
       case DOUBLE_TYPE: out_delta._data.double_value -= other._data.double_value; break;
       default: break;
      }
    }
  }
} // namespace graphlab
