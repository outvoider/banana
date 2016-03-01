#ifndef BANANA_H
#define BANANA_H

#include <iostream>
#include <algorithm>
#include <memory>
#include <json/json.h>
#include <sybfront.h>	/* sybfront.h always comes first */
#include <sybdb.h>	/* sybdb.h is the only other file you need */
#include <fstream>
#include <ctpublic.h>
#include <regex>
#include "spdlog/spdlog.h"
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include "semaphore.hpp"

#include "client_http.hpp"
typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

#include "lmdb-client.hpp"

#include "tdspp.hh"  /* ct-lib cpp wrapper, <<not using, only for testing>> */

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
      initialize unique ptrs
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
    shared_ptr<TDSRows> rows;
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
    channel(const string& n, const Json::Value& j) : name(n), topics(j) {}
    ~channel(){}
  private:
    
  };

  static vector<banana::channel> channels;

  struct man {
    std::string env = "dev";
    Json::Value globalConfig;
    vector<banana::channel> channels;
    man(Json::Value& _globalConfig, vector<banana::channel>& _channels, std::string _env) :globalConfig(_globalConfig), channels(_channels), env(_env){}
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

  auto loadConfigFile = []()->int{

    Json::Reader reader;
    ifstream ifs("config.json");

    if (ifs.is_open()){
      istream& s = ifs;
      bool parsingSuccessful = reader.parse(s, globalConfig);
      if (!parsingSuccessful){
        spdlog::get("logger")->error() << "Failed to parse configuration\n";
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
    int fieldcount = q->fieldcount;
    
    while (!q->eof()) {
    
      Json::Value meta;
      Json::Value body;
      stringstream ss;

      boost::uuids::uuid uuid = boost::uuids::random_generator()();

      meta["index"]["_index"] = "cdc";
      meta["index"]["_type"] = topic["name"].asString();
      meta["index"]["_id"] = boost::uuids::to_string(uuid);

      for (int i = 0; i < fieldcount; i++){
        auto n = q->fields(i)->colname;
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

      auto mdb = std::make_shared<::LMDBClient>();
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

  auto executeScript = [](const string channelName, const Json::Value& topic, string& script)->shared_ptr <banana::TDSClient> {

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

  auto processSqlResultsProxy = [](const string channelName, const Json::Value& topic, shared_ptr<Json::Value> root)->shared_ptr<vector<string>> {

    auto vs = make_shared<vector<string>>();
    
    if (root == nullptr || root->isNull()){
      return vs;
    }

    //do stuff
    string currentLastStartTime;
    
    Json::Value::iterator itr = root->begin();
    while (itr != root->end()) {
      Json::Value j = (*itr);

      Json::Value meta;
      Json::Value body(j);
      stringstream ss;

      boost::uuids::uuid uuid = boost::uuids::random_generator()();

      meta["index"]["_index"] = "cdc";
      meta["index"]["_type"] = topic["name"].asString();
      meta["index"]["_id"] = boost::uuids::to_string(uuid);

      //copy
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

      if (topic["log"].isNull() || (topic["log"].isBool() && topic["log"].asBool())){
        spdlog::get("logger")->info() << ss.str();
      }        

      vs->push_back(ss.str());

      itr++;

    }

    if (vs->size() > 0){
      string nm = topic["name"].asString();

      ::LMDBClient mdb;
      mdb.setLmdbValue(nm, currentLastStartTime);
    }

    return vs;
  };

  auto executeScriptProxy = [](const string& channelName, const Json::Value& topic, string& script)->shared_ptr<Json::Value> {

    shared_ptr<Json::Value> vs = nullptr;
    
    auto conn = globalConfig["sqlproxy"][channelName][::env];

    if (conn.isNull()){
      spdlog::get("logger")->error() << "config.json does not define sqlproxy." + channelName + "." + ::env;
      return vs;
    }

    HttpClient client(conn["host"].asString() + ":" + conn["port"].asString());

    shared_ptr<SimpleWeb::ClientBase<SimpleWeb::HTTP>::Response> r1;

    try {

      Json::Value query;
      query["request"] = script;
      
      Json::FastWriter fastWriter;
      std::string qt = fastWriter.write(query);
      stringstream buf;
      buf << qt;

      std::map<string, string> header;
      header["Content-Type"] = "application/json";

      r1 = client.request("POST", "/adhoc-query", buf, header);

      //get response
      stringstream ss;
      ss << r1->content.rdbuf();
      
      if (ss.str().size() == 0){
        return vs;
      }
      
      vs = make_shared<Json::Value>();

      ss >> *vs;

      r1->content.clear();

    }
    catch (const exception& e){
      spdlog::get("logger")->error() << e.what();
      spdlog::get("logger")->flush();
      return vs;
    }

    return vs;
  };

  auto processSqlResults = [](const string channelName, const Json::Value& topic, shared_ptr<banana::TDSClient> db)->shared_ptr<vector<string>> {

    auto vs = make_shared<vector<string>>();
    
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

      vs->push_back(ss.str());
    }

    //Store this in lmdb
    //
    //Only update if rows returned
    //
    if (vs->size() > 0){
      string nm = topic["name"].asString();

      auto mdb = std::make_shared<::LMDBClient>();
      mdb->setLmdbValue(nm, currentLastStartTime);
    }

    return vs;
  };

  auto processTopic = [](string& channelName, Json::Value& topic)->shared_ptr<vector<string>> {

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

    ::LMDBClient mdb;
    string storedLastStartTime = mdb.getLmdbValue(nm);

    std::regex e("\\$\\(LAST_EXEC_TIME\\)");
    script = scriptss.str();
    script = std::regex_replace(script, e, storedLastStartTime.size() == 0 ? defaultLastExecTime : "convert(datetime, '" + storedLastStartTime + "')");

    //if using sql-proxy
    auto j = executeScriptProxy(channelName, topic, script);

    //if using ct-lib
    //auto vs = executeScriptCT(channelName, topic, script);

    //
    //  process the results with tds wrapper, get pointer to processed results
    //
    //if using db-lib
    //auto vs = processSqlResults(channelName, topic, db);

    //if using sql-proxy
    auto vs = processSqlResultsProxy(channelName, topic, j);
    
    //if using db-lib destroy
    //db.reset();

    //
    //  log work
    if (vs->size() > 0) {
      stringstream ss;
      ss << "Topic => " << topic["name"].asString() << " completed. " << " Total => " << vs->size() << " Elapsed = > ";
      string msg = ss.str();
      timer(msg, t1);
    }
    
    return vs;
  };

  auto bulkToElastic = [](vector<string>& v)->int{

    if (v.size() == 0){
      return 1;
    }

    stringstream ss;

    for (auto& s : v){
      ss << s;
    }

    auto esHost = globalConfig["es"][::env]["host"].asString();
    auto esPort = globalConfig["es"][::env]["port"].asString();

    //start timer
    auto t1 = std::chrono::high_resolution_clock::now();

    HttpClient bulkClient(esHost + ":" + esPort);
    auto r = bulkClient.request("POST", "/_bulk", ss);
    stringstream o;
    o << r->content.rdbuf();

    Json::Value jv;
    o >> jv;
    if (!jv["error"].isNull()){
      spdlog::get("logger")->info() << o.str();
    }
    
    ss.str("");
    ss << "Bulk to ES completed.  Total => " << v.size() << " Elapsed =>";
    string msg = ss.str();
    timer(msg, t1);

    return 0;
  };

  auto processChannel = [](banana::channel& channel_ptr)->int{

    vector<string> combined;

    auto pr = channel_ptr;
    auto channel = pr.topics;

    for (int index = 0; index < channel.size(); ++index){
      auto vs = processTopic(pr.name, channel[index]);
      combined.insert(combined.end(), vs->begin(), vs->end());       
    }

    //When all is done, bulkload to elasticsearch
    if (combined.size() > 0){
      bulkToElastic(combined); 
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