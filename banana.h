#ifndef BANANA_H
#define BANANA_H

#include <memory>
#include <json/json.h>
//#include "lmdb.h"
#include <sybfront.h>	/* sybfront.h always comes first */
#include <sybdb.h>	/* sybdb.h is the only other file you need */

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

  class TDSClient{
  public:
    struct COL
    {
      char *name;
      char *buffer;
      int type, size, status;
    } *columns, *pcol;
    int init();
    int connect();
    int connect(string& _host, string& _user, string& _pass);
    int useDatabase(string& _db);
    void sql(string& _script);
    int execute();
    int getMetadata();
    int fetchData();
    TDSClient(){};
    vector<shared_ptr<string>> fieldNames;
    vector<vector<shared_ptr<string>>> fieldValues;
    TDSClient(string& _host, string& _user, string& _pass) : host(_host), user(_user), pass(_pass) {}
    ~TDSClient() {
      dbexit();      
    }
  private:
    string host;
    string user;
    string pass;
    string script;
    int ncols;
    int row_code;
    LOGINREC *login;
    DBPROCESS *dbproc;
    RETCODE erc;

  };

};

//static MDB_env *lmdb_env;
//static MDB_dbi lmdb_dbi;

#endif