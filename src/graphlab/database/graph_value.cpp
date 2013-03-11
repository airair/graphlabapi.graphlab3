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

  void graph_value::init(graph_datatypes_enum type) {
    _type = type; 
    _null_value = true;
    _modified = false;
    _use_delta_commit = false;
    memset(&_data, 0, sizeof(_data));
    memset(&_old, 0, sizeof(_old));
    switch(type) {
     case STRING_TYPE:
     case BLOB_TYPE: _len = 0;  break;
     case DOUBLE_TYPE: _len = sizeof(graph_double_t); break;
     case INT_TYPE: _len =sizeof(graph_int_t); break;
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
      if (_old.bytes != NULL) {
        free(_old.bytes);
        _old.bytes = NULL;
      }
    }
  }

  const void* graph_value::get_raw_pointer() {
    if (is_null()) {
      return NULL;
    } else if (is_scalar_graph_datatype(_type)) {
      return reinterpret_cast<void*>(&_data);
    } else {
      return reinterpret_cast<void*>(_data.bytes);
    }
  }

  void* graph_value::get_mutable_raw_pointer() {
    if (is_null()) {
      return NULL;
    } else if (is_scalar_graph_datatype(_type)) {
      _modified = true;
      return reinterpret_cast<void*>(&_data);
    } else {
      _modified = true;
      return reinterpret_cast<void*>(_data.bytes);
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


  bool graph_value::set_val(const char* val, size_t length, bool delta) {
    switch(_type) {
     case INT_TYPE:
       {
         graph_int_t intval = *((graph_int_t*)val);
         return delta ? set_integer(intval + _old.int_value) 
             : set_integer(intval);
       }
     case DOUBLE_TYPE:
       {
         graph_double_t doubleval = *((graph_double_t*)val);
         return delta ? set_double(doubleval + _old.double_value) 
             : set_double(doubleval);
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
         graph_int_t intval = boost::lexical_cast<graph_int_t>(val_str); 
         return delta ? set_integer(intval + _old.int_value) 
             : set_integer(intval);
       }
     case DOUBLE_TYPE:
       {
         graph_double_t doubleval = boost::lexical_cast<graph_double_t>(val_str);
         return delta ? set_double(doubleval + _old.double_value) 
             : set_double(doubleval);
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


  bool graph_value::set_integer(graph_int_t val) {
    if (type() == INT_TYPE) {
      // if the modified flag was already set, leave it.
      // otherwise, set it only if the value changed.
      _modified = (_modified || _data.int_value != val || _null_value);
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
      _modified = (_modified || _data.double_value != val || _null_value);
      _data.double_value = val;
      _null_value = false;
      return true;
    } else {
      return false;
    }
  }

  bool graph_value::set_vid(graph_vid_t val) {
    if (type() == VID_TYPE) {
      // if the modified flag was already set, leave it.
      // otherwise, set it only if the value changed.
      _modified = (_modified || _data.vid_value != val || _null_value);
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

  /**
   * Make a deep copy into out_value. Ignore all fields in the out_value.
   */
  void graph_value::deepcopy(graph_value& out_value) {
    out_value._type = _type;
    out_value._data = _data;
    out_value._old = _old;
    out_value._len = _len;
    out_value._null_value = _null_value;
    out_value._modified = _modified;
    out_value._use_delta_commit = _use_delta_commit;
    if ((_type == STRING_TYPE || _type == BLOB_TYPE) && _data.bytes != NULL) {
      out_value._data.bytes = (char*) malloc(_len);
      memcpy((void*)(out_value._data.bytes), (void*)(_data.bytes), _len);

      out_value._old.bytes = (char*) malloc(_len);
      memcpy((void*)(out_value._old.bytes), (void*)(_old.bytes), _len);
    }
  }
} // namespace graphlab
