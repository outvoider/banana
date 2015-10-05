#ifndef BANANA_H
#define BANANA_H

#include <iostream>
#include <memory>
#include <json/json.h>
#include <sybfront.h>	/* sybfront.h always comes first */
#include <sybdb.h>	/* sybdb.h is the only other file you need */

//#include <iostream>
#include <fstream>
//#include <json/json.h>
//#include "banana.h"

#include <ctpublic.h>
//#include <memory>
#include <regex>
#include "spdlog/spdlog.h"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "semaphore.hpp"

#include "client_http.hpp"
typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

//#include "lmdb.h"
#include "lmdb-client.hpp"

using namespace std;

static Json::Value globalConfig;
static string defaultLastExecTime = "CONVERT(datetime, '1970-01-01')";
static unsigned int sleep_ms = 5000;
static string env = "dev";

namespace {
  auto loadConfigFile = []()->int{

    Json::Reader reader;
    ifstream ifs("config.json");

    if (ifs.is_open()){
      istream& s = ifs;
      bool parsingSuccessful = reader.parse(s, globalConfig);
      if (!parsingSuccessful){
        spdlog::get("logger")->error() << "Failed to parse configuration\n"
          << reader.getFormattedErrorMessages();
        return 1;
      }
    }
    else {
      spdlog::get("logger")->error() << "config.json cannot be found";
      return 1;
    }
    ifs.close();

    return 0;

  };

  auto timer = [](string& message, std::chrono::time_point<std::chrono::system_clock>& t1){

    stringstream ss;
    auto t2 = std::chrono::high_resolution_clock::now();
    ss.str("");
    ss << message << " "
      << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
      << " milliseconds\n";

    spdlog::get("logger")->info(ss.str());
  };
}

namespace banana {
  class channel {
  public:
    string name;
    Json::Value topics;
    channel(const string& n, const Json::Value& j) : name(n), topics(j) {}
    ~channel(){}
  };

  static vector<banana::channel> channels;

  template<typename T>
  class TDSCell {
  public:
    T value;
    TDSCell<T>(T _val):value(_val){}
    ~TDSCell<T>(){}
  };

  typedef vector<shared_ptr<TDSCell<string>>> RowOfString;
  typedef vector<shared_ptr<vector<shared_ptr<TDSCell<string>>>>> TableOfRowsOfString;

  class TDSRows {
  public:  
    shared_ptr<RowOfString> fieldNames;
    shared_ptr<TableOfRowsOfString> fieldValues;
    TDSRows() {
      /*
      initialize shared ptrs
      */
      fieldNames = make_shared<RowOfString>();
      fieldValues = make_shared<TableOfRowsOfString>();
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
    
    //vector<char*> buffers;
    //vector<int> nullbind;

    int init();
    int connect();
    int connect(string& _host, string& _user, string& _pass);
    int useDatabase(string& _db);
    void sql(string& _script);
    int execute();
    int getMetadata();
    int fetchData();
    void close();
    TDSClient(){};
    unique_ptr<TDSRows> rows;
    TDSClient(string& _host, string& _user, string& _pass) : host(_host), user(_user), pass(_pass) {}
    ~TDSClient();
  private:
    string host;
    string user;
    string pass;
    string script;
    int ncols;
    int row_code;
    LOGINREC *login = NULL;
    DBPROCESS *dbproc = NULL;
    RETCODE erc;

  };
  
  struct man {
    std::string env = "dev";
    Json::Value globalConfig;
    vector<banana::channel> channels;
    man(Json::Value& _globalConfig, vector<banana::channel>& _channels, std::string _env) :globalConfig(_globalConfig), channels(_channels), env(_env){}
    //void timer(string& message, std::chrono::time_point<std::chrono::system_clock>& t1);
    shared_ptr<vector<shared_ptr<string>>> processSqlResults(const string channelName, const Json::Value& topic, shared_ptr<banana::TDSClient> db);
    shared_ptr<banana::TDSClient> executeScript(const string channelName, const Json::Value& topic, string& script);
    shared_ptr<vector<shared_ptr<string>>> processTopic(string& channelName, Json::Value& topic);
    int bulkToElastic(shared_ptr<vector<shared_ptr<string>>>& v);
    int processChannel(banana::channel& channel_ptr);
    int start();

  };

};

#endif