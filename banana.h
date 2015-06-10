#ifndef BANANA_H
#define BANANA_H

#include <json/json.h>
#include "jsoncons/json.hpp"

static Json::Value globalConfig;
static jsoncons::basic_json<char, std::allocator<void>> gConfig;

#endif BANANA_H