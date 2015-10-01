#include <iostream>
#include <fstream>
#include <json/json.h>
#include "banana.h"

#include <ctpublic.h>
#include <memory>
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

#include "FreeTDSHelper.h"

using namespace std;

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
    
  auto executeScript = [](const string channelName, const Json::Value& topic, string& script, vector<shared_ptr<string>>& vs)->int {

    //vector<shared_ptr<string>> vs;

    auto conn = globalConfig["connection"][channelName][::env];

    FreeTDSHelper helper;

    if (helper.openDB((char*)conn["host"].asString().c_str(), (char*)conn["user"].asString().c_str(), (char*)conn["pass"].asString().c_str(), (char*)conn["database"].asString().c_str()) == false){
      return -1;
    }

    //http://blog.csdn.net/qwidget/article/details/6444829  
    if (helper.query((char*)script.c_str()) == -1)
    {
      printf("%s/n", helper.getErrorMessage());
      return -1;
    }
    printf("there are %d rows, %d columns/n", helper.getRowCount(), helper.getColumnCount());
    
    for (int i = 0; i<helper.getRowCount(); ++i)
    {
      ROW* r = helper.getRow(i);
      for (int j = 0; j<helper.getColumnCount(); ++j)
      {
        printf("%s/t", r->getField(j));
      }
      printf("/n");
    }
    helper.releaseResultSet();  

    return 0;

    //

    int rc;

    auto db = unique_ptr<banana::TDSClient>(new banana::TDSClient());

    rc = db->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
    if (rc)
      return rc;
    
    rc = db->useDatabase(conn["database"].asString());
    if (rc)
      return rc;

    db->sql(script);

    rc = db->execute();
    if (rc)
      return rc;
    
    //do stuff
    string currentLastStartTime;
    //int fieldcount = db->fieldNames.size();
    int fieldcount = db->rows->fieldNames->size();

    //for (auto& row : db->fieldValues){
    for (auto& row : *(db->rows->fieldValues)){
      
      Json::Value meta;
      Json::Value body;
      stringstream ss;

      boost::uuids::uuid uuid = boost::uuids::random_generator()();

      meta["index"]["_index"] = "cdc";
      meta["index"]["_type"] = topic["name"].asString();
      meta["index"]["_id"] = boost::uuids::to_string(uuid);

      for (int i = 0; i < fieldcount; i++){
        //body[*db->fieldNames.at(i)] = *row.at(i);
        //body[*db->fieldNamesPtr->at(i)] = *row.at(i);
        auto n = db.get()->rows->fieldNames->at(i)->value;
        body[db.get()->rows->fieldNames->at(i)->value] = row.get()->at(i)->value;
      }
      body["processed"] = 0;
      body["channel"] = channelName;
      
      /**
        which module(s) should this record be replicated to?
      **/
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
      vs.push_back(sv);
      
    }

    /*
    stringstream ss1;
    ss1 << "executeScript() Topic => " << topic["name"] << " total => " << vs.size();
    spdlog::get("logger")->info() << ss1.str();
    */

    //Store this in lmdb
    /**
    Only update if rows returned
    **/
    if (vs.size() > 0){
      string nm = topic["name"].asString();
      
      auto mdb = std::make_unique<::LMDBClient>();
      mdb->setLmdbValue(nm, currentLastStartTime);
    }

    return 0;
  };
  
  auto processTopic = [](string& channelName, Json::Value& topic, vector<shared_ptr<string>>& vs)->int{

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
    
    //do we really need to log the script itself?
    //spdlog::get("logger")->info() << script;

    auto rc = executeScript(channelName, topic, script, vs);

    stringstream ss;
    ss << "Topic => " << topic["name"].asString() << " completed. " << " Total => " << vs.size() << " Elapsed = > ";
    //timer("Topic => " + topic["name"].asString() + " completed.  Elapsed => ", t1);
    string msg = ss.str();
    timer(msg, t1);

    scriptss.str("");

    return 0;
  };

  auto bulkToElastic = [](vector<shared_ptr<string>>& v)->int{

    stringstream ss;
    
    for (auto& s : v){
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

    //debug detail, else no need
    
    Json::Value jv;
    o >> jv;
    if (!jv["error"].isNull()){
      spdlog::get("logger")->info() << o.str();
    }
    //spdlog::get("logger")->info() << "bulkToElastic() took " << jv["took"].asString() << "ms errors => " << jv["errors"].asString();
    
    ss.str("");
    ss << "Bulk to ES completed.  Total => " << v.size() << " Elapsed =>";
    string msg = ss.str();
    timer(msg, t1);

    return 0;
  };

  auto processChannel = [](unique_ptr<banana::channel>& channel_ptr)->int{
  
    vector<shared_ptr<string>> combined;

    auto pr = *channel_ptr;
    auto channel = pr.topics;
    
    for (int index = 0; index < channel.size(); ++index){      
      vector<shared_ptr<string>> vs;
      processTopic(pr.name, channel[index], vs);
      combined.insert(combined.end(), vs.begin(), vs.end());
      //vs.clear();
    }

    //When all is done, bulkload to elasticsearch
    if (combined.size() > 0){
      bulkToElastic(combined);
      //combined.clear();
    }
    
    return 0;
  };

  auto start = []()->int{

    auto channels = globalConfig["channels"];

    vector<unique_ptr<banana::channel>> v;
    
    vector<string> names;
    for (auto const& id : channels.getMemberNames()) {
      names.push_back(id);
    }

    for (auto i = 0; i < channels.size(); i++) {
      auto channel = channels[names[i]];
      string channelName = names[i];
      
      v.push_back(unique_ptr<banana::channel>(new banana::channel(channelName, channel)));
    }

    //parallel_for_each(v.begin(), v.end(), processChannel);
    std::for_each(v.begin(), v.end(), processChannel);
    
    //v.clear();

    return 0;
  };

};