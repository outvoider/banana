#ifndef BANANA_H
#define BANANA_H

#include <json/json.h>
#include "lmdb.h"

using namespace std;

static Json::Value globalConfig;
static string defaultLastExecTime = "CONVERT(datetime, '1970-01-01')";
static unsigned int sleep_ms = 5000;
static string env = "dev";

namespace banana {
  class channel {
  public:
    string name;
    Json::Value topics;
    channel(const string& n, const Json::Value& j) : name(n), topics(j) {}
    ~channel(){}
  };
};

static MDB_env *lmdb_env;
static MDB_dbi lmdb_dbi;

#endif