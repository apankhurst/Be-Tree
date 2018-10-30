#ifndef SERIALIZABLE_TYPES_H
#define SERIALIZABLE_TYPES_H

#include <iostream>
#include <string>
#include "swap_space.hpp"

struct SerializableString: public serializable, public std::string {

  SerializableString(): std::string() {}
  SerializableString(const char* ch) : std::string(ch) {}
  SerializableString(const std::string& str) : std::string(str) {}

  void _serialize(std::iostream &fs, serialization_context &context) {
    fs << "string ";
    fs << data();
  }

  void _deserialize(std::iostream &fs, serialization_context &context) {
    std::string dummy;
    fs >> dummy;
		*this = SerializableString(dummy);
  }
};

#endif
