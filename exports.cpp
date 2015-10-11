#include <iostream>
//#include <fstream>
//#include <json/json.h>
#include "banana.hpp"

//#include <ctpublic.h>
//#include <memory>
//#include <regex>
//#include "spdlog/spdlog.h"

//#include <boost/uuid/uuid.hpp>            // uuid class
//#include <boost/uuid/uuid_generators.hpp> // generators
//#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

//#include "semaphore.hpp"

//#include "client_http.hpp"
//typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

//#include "lmdb-client.hpp"

using namespace std;

namespace {
  
  /*
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
  
  */
  
}; // le fin anon namespace

/*
  banana::man
*/
/*
namespace banana {
  struct man {

    man(){}
    //void timer(string& message, std::chrono::time_point<std::chrono::system_clock>& t1);
    shared_ptr<vector<shared_ptr<string>>> processSqlResults(const string channelName, const Json::Value& topic, shared_ptr<banana::TDSClient> db);
    shared_ptr<banana::TDSClient> executeScript(const string channelName, const Json::Value& topic, string& script);
    shared_ptr<vector<shared_ptr<string>>> processTopic(string& channelName, Json::Value& topic);
    int bulkToElastic(shared_ptr<vector<shared_ptr<string>>>& v);
    int processChannel(banana::channel& channel_ptr);
    int start();

  };
}
*/

//implementations
/*
void banana::man::timer(string& message, std::chrono::time_point<std::chrono::system_clock>& t1){
stringstream ss;
auto t2 = std::chrono::high_resolution_clock::now();
ss.str("");
ss << message << " "
<< std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
<< " milliseconds\n";

spdlog::get("logger")->info(ss.str());
}
*/

/*
shared_ptr<vector<shared_ptr<string>>> banana::man::processSqlResults(const string channelName, const Json::Value& topic, shared_ptr<banana::TDSClient> db){
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

    //which module(s) should this record be replicated to?
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

  if (vs->size() > 0){
    string nm = topic["name"].asString();

    auto mdb = std::make_unique<::LMDBClient>();
    mdb->setLmdbValue(nm, currentLastStartTime);
  }

  return vs;
}

shared_ptr<banana::TDSClient> banana::man::executeScript(const string channelName, const Json::Value& topic, string& script){
  
  auto conn = this->globalConfig["connection"][channelName][this->env];
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
}

shared_ptr<vector<shared_ptr<string>>> banana::man::processTopic(string& channelName, Json::Value& topic){
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

  spdlog::get("logger")->info() << "Processing " << channelName << " | " << topic["name"];

  //execute script, will get instance of tds wrapper
  auto db = executeScript(channelName, topic, script);

  
  //process the results with tds wrapper, get pointer to processed results
  auto vs = processSqlResults(channelName, topic, db);

#ifdef _DEBUG
  _CrtMemState s1;
#endif

  //call destructor
  
#ifdef _DEBUG
  _CrtMemCheckpoint(&s1);
  _CrtMemDumpStatistics(&s1);
  //_CrtDumpMemoryLeaks();
#endif
  
  db.reset();

#ifdef _DEBUG
  _CrtMemCheckpoint(&s1);
  _CrtMemDumpStatistics(&s1);
  //_CrtDumpMemoryLeaks();
#endif

  //log work
  
  stringstream ss;
  ss << "Topic => " << topic["name"].asString() << " completed. " << " Total => " << vs->size() << " Elapsed = > ";
  string msg = ss.str();
  timer(msg, t1);

  return vs;
}

int banana::man::bulkToElastic(shared_ptr<vector<shared_ptr<string>>>& v){
  stringstream ss;

  for (auto& s : *v){
    ss << *s;
  }

  auto esHost = this->globalConfig["es"][this->env]["host"].asString();
  auto esPort = this->globalConfig["es"][this->env]["port"].asString();

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
}

int banana::man::processChannel(banana::channel& channel_ptr) {
  auto combined = make_shared<vector<shared_ptr<string>>>();

  auto pr = channel_ptr;
  auto channel = pr.topics;

  for (int index = 0; index < channel.size(); ++index){
    auto vs = processTopic(pr.name, channel[index]);
    combined->insert(combined->end(), vs->begin(), vs->end());
    //vs.reset();
  }

  //When all is done, bulkload to elasticsearch
  if (combined->size() > 0){
    bulkToElastic(combined);
    //combined.reset();
  }

  return 0;
}

int banana::man::start(){
//std::for_each(banana::channels.begin(), banana::channels.end(), processChannel);
//parallel_for_each(banana::channels.begin(), banana::channels.end(), ::processChannel);

for (auto& e : this->channels){
this->processChannel(e);
}

return 0;
}
*/

