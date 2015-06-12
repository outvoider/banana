#ifndef BANANA_H
#define BANANA_H

#include <json/json.h>
#include "jsoncons/json.hpp"

using namespace std;

static Json::Value globalConfig;
static jsoncons::basic_json<char, std::allocator<void>> gConfig;

namespace banana {
  class channel {
  public:
    string name;
    Json::Value topics;
    channel(const string& n, const Json::Value& j) : name(n), topics(j) {}
    ~channel(){}
  };
};

#endif BANANA_H