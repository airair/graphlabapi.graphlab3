#include <iostream>
#include <graphlab/zookeeper/key_value.hpp>

void callback(graphlab::zookeeper::key_value* kv,
              const std::vector<std::string>& newkeys,
              const std::vector<std::string>& deletedkeys,
              const std::vector<std::string>& modifiedkeys) {
  std::cout << "Watch triggered\n";
  if (deletedkeys.size() > 0) {
    std::cout << "Deleted Keys: \n";
    for (size_t i = 0;i < deletedkeys.size(); ++i) {
      std::cout << "\t" << deletedkeys[i] << "\n";
    }
  }
  if (newkeys.size() > 0) {
    std::cout << "New Keys: \n";
    for (size_t i = 0;i < newkeys.size(); ++i) {
      std::pair<bool, std::string> val = kv->get(newkeys[i]);
      if (val.first) {
        std::cout << "\t" << newkeys[i] << " = " << val.second << "\n";
      }
      else {
        std::cout << "\t" << newkeys[i] << " = ???" << "\n";
      }
    }
  }
  if (modifiedkeys.size() > 0) {
    std::cout << "Modified Keys: \n";
    for (size_t i = 0;i < modifiedkeys.size(); ++i) {
      std::pair<bool, std::string> val = kv->get(modifiedkeys[i]);
      if (val.first) {
        std::cout << "\t" << modifiedkeys[i] << " = " << val.second << "\n";
      }
      else {
        std::cout << "\t" << modifiedkeys[i] << " = ???" << "\n";
      }
    }
  }
}

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cout << "Usage: zookeeper_test [zkhost] [prefix] [name]\n";
    return 0;
  }
  std::string zkhost = argv[1];
  std::string prefix = argv[2];
  std::string name = argv[3];
  std::vector<std::string> zkhosts; zkhosts.push_back(zkhost);
  graphlab::zookeeper::key_value key_value(zkhosts, prefix, name);

  std::cout << "Commands: \n";
  std::cout << "Set: s [key] [value]\n";
  std::cout << "Modify: m [key] [value]\n";
  std::cout << "Get: g [key]\n";
  std::cout << "Quit: q\n";
  std::cout << "\n\n";


  key_value.set_callback(callback);

  while(1) {
    char command;
    std::string key, value;

    std::cin >> command;
    if (command == 'q') break;
    else if (command == 's' || 
             command == 'm') std::cin >> key >> value;
    else if (command == 'g') std::cin >> key;

    if (command == 's') {
      if (key_value.insert(key, value) == false) {
        std::cout << "\t Insertion failure\n";
      }
    } 
    else if (command == 'm') {
      if (key_value.modify(key, value) == false) {
        std::cout << "\t Modification failure\n";
      }
    }
    else if (command == 'g') {
      std::pair<bool, std::string> val = key_value.get(key);
      if (val.first == false) {
        std::cout << "\t???\n";
      }
      else {
        std::cout << "\t" << val.second << "\n";
      }
    }

  }
}
