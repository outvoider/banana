#ifndef BANANA_H
#define BANANA_H

#include <iostream>
#include <algorithm>
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

#include "tdspp.hh"

using namespace std;

static Json::Value globalConfig;
static string defaultLastExecTime = "CONVERT(datetime, '1970-01-01')";
static unsigned int sleep_ms = 5000;
static string env = "dev";

namespace banana {

  template<typename T>
  class TDSCell {
  public:
    T value;
    TDSCell<T>(T _val) : value(_val){}
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
    /*
    struct COL
    {
    char *name;
    char *buffer;
    int type, size, status;
    } *columns, *pcol;
    */

    struct COL {						/* (1) */
      char *name;
      int type, status;
      size_t _size;
      std::string buffer;
      COL(char* name, int type, size_t size) : name(name), type(type), _size(size) {
        // note { string s; s.resize(5); strcpy(&s[0], "123"); s.resize(strlen(s.data())); cout << s; }
        buffer.resize(_size + 1);
        if (::strlen(name) > size)
          _size = ::strlen(name);
      }
      bool operator== (const COL& r) const {
        return name == r.name && type == r.type && _size == r._size && status == r.status && buffer == r.buffer;
      }
      const char* lexical() const {
        return buffer.data();
      }
      int size() const { return _size; }
      bool isNULL() const { return status == -1; }
    };
    typedef std::vector<COL> Values;

    //vector<char*> buffers;
    //vector<int> nullbind;
    Values values;

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
    DBPROCESS *dbproc = NULL;
  private:
    string host;
    string user;
    string pass;
    string script;
    int ncols;
    int row_code;
    LOGINREC *login = NULL;    
    RETCODE erc;
  };

  class channel {
  public:
    friend bool operator== (const channel &n1, const channel &n2){
      return n1.name == n2.name;
    };
    string name;
    Json::Value topics;
    shared_ptr<TDSClient> client;
    channel(const string& n, const Json::Value& j) : name(n), topics(j) {
    
      //create a new TDSClient per channel
      auto conn = globalConfig["connection"][name][::env];
      client = make_shared<TDSClient>();
      client->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
      client->useDatabase(conn["database"].asString());
      
    }
    ~channel(){}
  private:
    
  };

  static vector<banana::channel> channels;

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

/*
  no namespace
*/
namespace {

  /*
  auto tdsClientApiInvoke = [](std::string& channelName, Json::Value& topic, std::string& script)->int{

    auto conn = globalConfig["connection"][channelName][::env];
    int rc;
    std::vector<std::string> queries = { script };
    rc = ::tdsClientExecute(conn["host"].asString(), conn["database"].asString(), conn["user"].asString(), conn["pass"].asString(), queries);

    return 0;
  };
  */

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

  auto processSqlResultsCT = [](const string channelName, const Json::Value& topic, Query* q)->shared_ptr<vector<shared_ptr<string>>> {

    auto vs = shared_ptr<vector<shared_ptr<string>>>(new vector<shared_ptr<string>>());

    //do stuff
    string currentLastStartTime;
    //int fieldcount = db->rows->fieldNames->size();

    int fieldcount = q->fieldcount;
    
    //vector<string> fieldNames;
    //for (int i; i < fieldcount; i++){
    //  fieldNames.push_back(q->fields(i)->colname);
    //}

    /* Print table headers, ie column names. */
    /*
    q->rows->printheader();
    while (!q->eof()) {
      cout << "| ";
      for (int i = 0; i < q->fieldcount; i++)
        cout << q->fields(i)->tostr() << " | ";
      cout << endl;
      q->next();
    }
    */

    while (!q->eof()) {
    //for (auto& row : *(db->rows->fieldValues)){

      Json::Value meta;
      Json::Value body;
      stringstream ss;

      boost::uuids::uuid uuid = boost::uuids::random_generator()();

      meta["index"]["_index"] = "cdc";
      meta["index"]["_type"] = topic["name"].asString();
      meta["index"]["_id"] = boost::uuids::to_string(uuid);

      for (int i = 0; i < fieldcount; i++){
        //auto n = db.get()->rows->fieldNames->at(i)->value;
        auto n = q->fields(i)->colname;
        //body[db.get()->rows->fieldNames->at(i)->value] = row.get()->at(i)->value;
        body[n] = q->fields(i)->tostr();
      }
      body["processed"] = 0;
      body["channel"] = channelName;

      //
      //  which module(s) should this record be replicated to?
      //
      if (topic["targetStores"].isArray()){
        body["targetStores"] = topic["targetStores"];
      }
      body["modelName"] = topic["modelName"].asString();

      //Update the start time
      currentLastStartTime = body["start_time"].asString();

      //.write will tack on a \n after completion
      Json::FastWriter writer;
      auto compactMeta = writer.write(meta);
      auto compactBody = writer.write(body);

      ss << compactMeta << compactBody;

      spdlog::get("logger")->info() << ss.str();

      auto sv = shared_ptr<string>(new string(ss.str()));
      vs->push_back(sv);

      q->next();
    }
    
    if (vs->size() > 0){
      string nm = topic["name"].asString();

      auto mdb = std::make_unique<::LMDBClient>();
      mdb->setLmdbValue(nm, currentLastStartTime);
    }

    return vs;
  };

  auto executeScriptCT = [](const string channelName, const Json::Value& topic, string& script)->shared_ptr<vector<shared_ptr<string>>> {

    shared_ptr<vector<shared_ptr<string>>> vs = nullptr;

    auto conn = globalConfig["connection"][channelName][::env];

    TDSPP *db = new TDSPP();
    try {
      /* Connect to database. */
      db->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
      /* Execute command. */
      db->execute("use " + conn["database"].asString());
      /* Create query. */
      Query *q = db->sql(script);

      try {
        /* Execute SQL query. */
        q->execute();
        
        vs = processSqlResultsCT(channelName, topic, q);

      }
      catch (TDSPP::Exception &e) {
        cerr << e.message << endl;
      }
      delete q;
    }
    catch (TDSPP::Exception &e) {
      cerr << e.message << endl;
    }
    delete db;

    return vs;
  };

  auto executeScript2 = [](const string channelName, const Json::Value& topic, string& script)->shared_ptr <banana::TDSClient> {

    auto conn = globalConfig["connection"][channelName][::env];
    int rc;
    auto db = shared_ptr<banana::TDSClient>(new banana::TDSClient());
    rc = db->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
    if (rc)
      return db;
    rc = db->useDatabase(conn["database"].asString());
    if (rc)
      return db;

    db->sql(script);

    db->execute();

    return db;
  };

  auto executeScript = [](const string channelName, const Json::Value& topic, string& script)->shared_ptr <banana::TDSClient> {

    //auto conn = globalConfig["connection"][channelName][::env];
    //int rc;
    //auto db = shared_ptr<banana::TDSClient>(new banana::TDSClient());
    //rc = db->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
    //if (rc)
    //  return db;
    //rc = db->useDatabase(conn["database"].asString());
    //if (rc)
    //  return db;

    auto itr = std::find_if(banana::channels.begin(), banana::channels.end(), [&channelName](banana::channel const& c){
      return c.name == channelName;
    });
    
    auto db = itr->client;

    db->sql(script);

    db->execute();

    return db;
  };

  auto processSqlResults = [](const string channelName, const Json::Value& topic, shared_ptr<banana::TDSClient> db)->shared_ptr<vector<shared_ptr<string>>> {

    auto vs = shared_ptr<vector<shared_ptr<string>>>(new vector<shared_ptr<string>>());

    //do stuff
    string currentLastStartTime;
    int fieldcount = db->rows->fieldNames->size();

    for (auto& row : *(db->rows->fieldValues)){

      Json::Value meta;
      Json::Value body;
      stringstream ss;

      boost::uuids::uuid uuid = boost::uuids::random_generator()();

      meta["index"]["_index"] = "cdc";
      meta["index"]["_type"] = topic["name"].asString();
      meta["index"]["_id"] = boost::uuids::to_string(uuid);

      for (int i = 0; i < fieldcount; i++){
        auto n = db.get()->rows->fieldNames->at(i)->value;
        body[db.get()->rows->fieldNames->at(i)->value] = row.get()->at(i)->value;
      }
      body["processed"] = 0;
      body["channel"] = channelName;

      //
      //  which module(s) should this record be replicated to?
      //
      if (topic["targetStores"].isArray()){
        body["targetStores"] = topic["targetStores"];
      }
      body["modelName"] = topic["modelName"].asString();

      //Update the start time
      currentLastStartTime = body["start_time"].asString();

      //.write will tack on a \n after completion
      Json::FastWriter writer;
      auto compactMeta = writer.write(meta);
      auto compactBody = writer.write(body);

      ss << compactMeta << compactBody;

      spdlog::get("logger")->info() << ss.str();

      auto sv = shared_ptr<string>(new string(ss.str()));
      vs->push_back(sv);

    }

    //
    //stringstream ss1;
    //ss1 << "executeScript() Topic => " << topic["name"] << " total => " << vs.size();
    //spdlog::get("logger")->info() << ss1.str();
    //

    //Store this in lmdb
    //
    //Only update if rows returned
    //
    if (vs->size() > 0){
      string nm = topic["name"].asString();

      auto mdb = std::make_unique<::LMDBClient>();
      mdb->setLmdbValue(nm, currentLastStartTime);
    }

    return vs;
  };

  auto processTopic = [](string& channelName, Json::Value& topic)->shared_ptr<vector<shared_ptr<string>>>{

    //start timer
    auto t1 = std::chrono::high_resolution_clock::now();

    stringstream scriptss;
    string script;

    auto scriptArr = topic["script"];
    for (auto& e : scriptArr){
      scriptss << e.asString();
    }

    //Fetch from lmdb store
    string nm = topic["name"].asString();

    auto mdb = std::make_unique<::LMDBClient>();
    string storedLastStartTime = mdb->getLmdbValue(nm);

    std::regex e("\\$\\(LAST_EXEC_TIME\\)");
    script = scriptss.str();
    script = std::regex_replace(script, e, storedLastStartTime.size() == 0 ? defaultLastExecTime : "convert(datetime, '" + storedLastStartTime + "')");

    //auto rc = tdsClientApiInvoke(channelName, topic, script);

    //auto vs = make_shared<vector<shared_ptr<string>>>();

    //do we really need to log the script itself?
    //spdlog::get("logger")->info() << script;

    //
    //  execute script, will get instance of tds wrapper
    //
    //if using db-lib
    auto db = executeScript(channelName, topic, script);

    //if using ct-lib
    //auto vs = executeScriptCT(channelName, topic, script);

    //
    //  process the results with tds wrapper, get pointer to processed results
    //
    //if using db-lib
    auto vs = processSqlResults(channelName, topic, db);

    //destroy
    //db.reset();

    //
    //  log work
    //
    if (vs != nullptr){
      stringstream ss;
      ss << "Topic => " << topic["name"].asString() << " completed. " << " Total => " << vs->size() << " Elapsed = > ";
      string msg = ss.str();
      timer(msg, t1);
    }    

    return vs;
  };

  auto bulkToElastic = [](shared_ptr<vector<shared_ptr<string>>>& v)->int{

    if (v == nullptr){
      return 1;
    }

    stringstream ss;

    for (auto& s : *v){
      ss << *s;
    }

    auto esHost = globalConfig["es"][::env]["host"].asString();
    auto esPort = globalConfig["es"][::env]["port"].asString();

    //start timer
    auto t1 = std::chrono::high_resolution_clock::now();

    HttpClient bulkClient(esHost + ":" + esPort);
    //std::map<string, string> header;
    //header["Content-Type"] = "application/json";
    auto r = bulkClient.request("POST", "/_bulk", ss);
    stringstream o;
    o << r->content.rdbuf();

    Json::Value jv;
    o >> jv;
    if (!jv["error"].isNull()){
      spdlog::get("logger")->info() << o.str();
    }
    //spdlog::get("logger")->info() << "bulkToElastic() took " << jv["took"].asString() << "ms errors => " << jv["errors"].asString();

    ss.str("");
    ss << "Bulk to ES completed.  Total => " << v->size() << " Elapsed =>";
    string msg = ss.str();
    timer(msg, t1);

    return 0;
  };

  auto processChannel = [](banana::channel& channel_ptr)->int{

    auto combined = make_shared<vector<shared_ptr<string>>>();

    auto pr = channel_ptr;
    auto channel = pr.topics;

    for (int index = 0; index < channel.size(); ++index){
      auto vs = processTopic(pr.name, channel[index]);
      if (vs != nullptr){
        combined->insert(combined->end(), vs->begin(), vs->end());
      }
      //vs.reset();
    }

    //When all is done, bulkload to elasticsearch
    if (combined->size() > 0){
      bulkToElastic(combined);
      //combined.reset();
    }

    return 0;
  };

  auto start = []()->int{

    //parallel_for_each(banana::channels.begin(), banana::channels.end(), ::processChannel);
    std::for_each(banana::channels.begin(), banana::channels.end(), ::processChannel);
    return 0;
  };

}

#endif