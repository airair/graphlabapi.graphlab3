#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <graphlab/zookeeper/server_list.hpp>
#include <iostream>
#include <algorithm>
extern "C" {
#include <zookeeper/zookeeper.h>
}

namespace graphlab {
namespace zookeeper {

// frees a zookeeper String_vector
static void free_String_vector(struct String_vector* strings) {
  if (strings->data) {
    for (size_t i = 0;i < strings->count; ++i) {
      free(strings->data[i]);
    }
    free(strings->data);
    strings->data = NULL;
    strings->count = 0;
  }
}

// convert a zookeeper String_vector to a c++ vector<string>
static std::vector<std::string> String_vector_to_vector(
    const struct String_vector* strings) {
  std::vector<std::string> ret;
  for (size_t i = 0;i < strings->count; ++i) {
    ret.push_back(strings->data[i]);
  }
  return ret;
}

// print a few zookeeper error status
static void print_stat(int stat, std::string prefix, std::string path) {
  if (stat == ZNONODE) {
    std::cerr << prefix << ": Node missing" << path << std::endl;
  }
  else if (stat == ZNOAUTH) {
    std::cerr << prefix << ": No permission to list children of node " 
              << path << std::endl;
  }
  else if (stat != ZOK) {
    std::cerr << prefix << ": Unexpected error " << stat 
              << " on path " << path << std::endl;
  }
}

// adds a trailing / to the path name if there is not one already
static std::string normalize_path(std::string prefix) {
  boost::algorithm::trim(prefix); 
  if (prefix.length() == 0) return "/";
  else if (prefix[prefix.length() - 1] != '/') return prefix + "/";
  else return prefix;
}

server_list::server_list(std::vector<std::string> zkhosts, 
                         std::string _prefix,
                         std::string _serveridentifier) : 
    prefix(_prefix), serveridentifier(_serveridentifier), callback(NULL) {
  // construct hosts list
  std::string hosts = boost::algorithm::join(zkhosts, ",");
  prefix = normalize_path(prefix);
  if (prefix[0] != '/') prefix = "/" + prefix;
  handle = zookeeper_init(hosts.c_str(), watcher, 10000, NULL, (void*)this, 0);
  // create the prefix if it does not already exist
  if (prefix != "/") create_dir(prefix.substr(0, prefix.length() - 1));

  assert(handle != NULL);

}

server_list::~server_list() {
  if (handle != NULL) zookeeper_close(handle);
}

void server_list::create_dir(std::string name) {
  int stat = zoo_create(handle, name.c_str(), NULL, -1, 
                       &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
  // we are ok with ZNODEEXISTS
  if (stat == ZOK || stat == ZNODEEXISTS) return;
  else print_stat(stat, "zk serverlist create_dir", name);
}

void server_list::delete_dir(std::string name) {
  int stat = zoo_delete(handle, name.c_str(), -1);
  // we are ok if the node is not empty in which case
  // there are still machines in the name space
  if (stat == ZOK || stat == ZNOTEMPTY) return;
  else print_stat(stat, "zk serverlist create_dir", name);
}




std::vector<std::string> server_list::get_all_servers(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  struct String_vector children;
  children.count = 0;
  children.data = NULL;

  std::vector<std::string> ret;

  // effective path is prefix + name_space
  std::string path = prefix + name_space;

  int stat = zoo_get_children(handle, path.c_str(), 0, &children);
  // if there are no children quit
  if (stat == ZNONODE) return ret;
  print_stat(stat, "zk serverlist get_all_servers", path);
  ret = String_vector_to_vector(&children);
  free_String_vector(&children); 
  return ret;
}

/// Joins a namespace
void server_list::join(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  create_dir(prefix + name_space);
  std::string path = normalize_path(prefix + name_space) + serveridentifier;
  int stat = zoo_create(handle, path.c_str(), NULL, -1, 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);
  print_stat(stat, "zk serverlist join", path);
  if (stat == ZNODEEXISTS) {
    std::cerr << "Server " << serveridentifier << " already exists!" << std::endl;
  }
  if (stat != ZOK) assert(false);
}

void server_list::leave(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  std::string path = normalize_path(prefix + name_space) + serveridentifier;
  zoo_delete(handle, path.c_str(), -1);
  // also try to delete its parents if they become empty
  delete_dir(prefix + name_space);
  delete_dir(prefix);
}


// ------------- watch implementation ---------------


std::vector<std::string> server_list::watch_changes(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  struct String_vector children;
  children.count = 0;
  children.data = NULL;
  std::vector<std::string> ret;

  std::string path = prefix + name_space;
  watchlock.lock();
  if (watches.count(path)) {
    watchlock.unlock();
    return get_all_servers(name_space);
  }
  watches.insert(path);

  int stat = zoo_get_children(handle, path.c_str(), 1, &children);
  watchlock.unlock();
  // if there are no children quit
  if (stat == ZNONODE) return ret;
  print_stat(stat, "zk serverlist watch_changes", path);
  ret = String_vector_to_vector(&children);
  free_String_vector(&children); 
  return ret;

}

void server_list::stop_watching(std::string name_space) {
  boost::algorithm::trim(name_space); assert(name_space.length() > 0);
  std::string path = prefix + name_space;
  watchlock.lock();
  watches.erase(path);
  watchlock.unlock();
}

void server_list::set_callback(boost::function<void(server_list*, 
                                                    std::string name_space,
                                                    std::vector<std::string> server)
                                              > fn) {
  watchlock.lock();
  callback = fn;
  watchlock.unlock();
}

void server_list::issue_callback(std::string path) {
  watchlock.lock();
  // search for the path in the watch set
  bool found = watches.count(path);
  if (found) {
    struct String_vector children;
    children.count = 0;
    children.data = NULL;
    std::vector<std::string> ret;
    // reissue the watch 
    int stat = zoo_get_children(handle, path.c_str(), 1, &children);
    print_stat(stat, "zk serverlist issue_callback", path);
    ret = String_vector_to_vector(&children);
    free_String_vector(&children); 

    // if a callback is registered
    if (callback != NULL) {
      callback(this, path, ret);
    }
  }
  watchlock.unlock();
}

void server_list::watcher(zhandle_t *zh, 
                          int type, 
                          int state, 
                          const char *path, 
                          void *watcherCtx) {
  server_list* slist = reinterpret_cast<server_list*>(watcherCtx);
  if (type == ZOO_CHILD_EVENT) {
    std::string strpath = path;
    slist->issue_callback(path);
  }
}




} // namespace zookeeper
} // namespace graphlab
