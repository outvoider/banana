#ifndef BANANA_H
#define BANANA_H

#include <iostream>
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

  class TDSRows {
  public:  
    shared_ptr<vector<shared_ptr<string>>> fieldNamesPtr;
    shared_ptr<vector<shared_ptr<vector<shared_ptr<string>>>>> fieldValuesPtr;
    TDSRows() {
      /*
      initialize shared ptrs
      */
      fieldNamesPtr = shared_ptr<vector<shared_ptr<string>>>(new vector<shared_ptr<string>>());
      fieldValuesPtr = make_shared<vector<shared_ptr<vector<shared_ptr<string>>>>>();
    }
    ~TDSRows(){
    }
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
    //vector<shared_ptr<string>> fieldNames;
    //vector<vector<shared_ptr<string>>> fieldValues;
    unique_ptr<TDSRows> rows;

    //shared_ptr<vector<shared_ptr<string>>> fieldNamesPtr;
    //shared_ptr<vector<shared_ptr<vector<shared_ptr<string>>>>> fieldValuesPtr;

    TDSClient(string& _host, string& _user, string& _pass) : host(_host), user(_user), pass(_pass) {}
    ~TDSClient();
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