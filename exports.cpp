#include <iostream>
#include <fstream>
#include <json/json.h>
#include "banana.h"

#include <ctpublic.h>
#include "tdspp.hh"
#include <memory>
#include <regex>
#include "spdlog/spdlog.h"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "semaphore.hpp"

#include "client_http.hpp"
typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

#include "lmdb.h"

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

  auto setLmdbValue = [](string& k, string& v){
    
    MDB_txn *txn;
    MDB_val key, data;

    int rc;
    rc = mdb_txn_begin(lmdb_env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, MDB_CREATE, &lmdb_dbi);

    key.mv_size = k.size();
    key.mv_data = (char*)k.c_str();
    data.mv_size = v.size();
    data.mv_data = (char*)v.c_str();
    
    rc = mdb_put(txn, lmdb_dbi, &key, &data, 0);
    rc = mdb_txn_commit(txn);

  };

  auto getLmdbValue = [](string& k)->string{

    MDB_txn *txn;
    MDB_val key, data;

    int rc;
    rc = mdb_txn_begin(lmdb_env, NULL, 0, &txn);
    rc = mdb_open(txn, NULL, MDB_CREATE, &lmdb_dbi);

    key.mv_size = k.size();
    key.mv_data = (char*)k.c_str();

    rc = mdb_get(txn, lmdb_dbi, &key, &data);
    if (rc != 0){
      mdb_txn_abort(txn);
      return "";
    }
    
    string res((const char*)data.mv_data, data.mv_size);

    rc = mdb_txn_commit(txn);
    return res;
  };

  auto executeScript = [](const string channelName, const Json::Value& topic, string& script)->vector<shared_ptr<string>>{

    vector<shared_ptr<string>> vs;
    
    auto conn = globalConfig["connection"][channelName][::env];
    auto db = unique_ptr<TDSPP>(new TDSPP());

    /* Connect to database. */
    try {
      db->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
    }
    catch (TDSPP::Exception &e){
      //cerr << e.message << endl;
      spdlog::get("logger")->error() << e.message;
      return vs;
    }

    /* Execute command. */
    db->execute("use " + conn["database"].asString());

    auto q = db->sql(script);
    
    try {
      /* Execute SQL query. */
      q->execute();

      vector<string> vec;
      Rows::RowList rl = q->rows->rows;
      for (auto& r : rl){
        for (auto& v : r){
          vec.push_back(v->colname);
        }
      }
      
      string currentLastStartTime;
      int rowCount = 0;
      
      while (!q->eof()) {

        Json::Value meta;
        Json::Value body;
        stringstream ss;

        boost::uuids::uuid uuid = boost::uuids::random_generator()();

        meta["index"]["_index"] = "cdc";
        meta["index"]["_type"] = topic["name"].asString();
        meta["index"]["_id"] = boost::uuids::to_string(uuid);
        
        for (int i = 0; i < q->fieldcount; i++){
          body[vec.at(i)] = q->fields(i)->tostr();
        }
        body["processed"] = 0;
        body["channel"] = channelName;
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
        
        q->next();
      }

      //Store this in lmdb
      stringstream ss1;
      ss1 << "executeScript() Topic => " << topic["name"] << " total => " << vs.size();
      spdlog::get("logger")->info() << ss1.str();
      /**
        Only update if rows returned
      **/
      if (vs.size() > 0){
        setLmdbValue(topic["name"].asString(), currentLastStartTime);
      }      

    }
    catch (TDSPP::Exception &e) {
      spdlog::get("logger")->error() << e.message;
    }
    catch (std::exception& e){
      spdlog::get("logger")->error() << e.what();
    }

    return vs;
  };

  auto processTopic = [](string& channelName, Json::Value& topic)->vector<shared_ptr<string>>{

    //start timer
    auto t1 = std::chrono::high_resolution_clock::now();

    stringstream scriptss;
    string script;
    
    auto scriptArr = topic["script"];
    for (auto& e : scriptArr){
      scriptss << e.asString();
    }

    //Fetch from lmdb store
    string storedLastStartTime = getLmdbValue(topic["name"].asString());

    std::regex e("\\$\\(LAST_EXEC_TIME\\)");
    script = scriptss.str();
    script = std::regex_replace(script, e, storedLastStartTime.size() == 0 ? defaultLastExecTime : "convert(datetime, '" + storedLastStartTime + "')");
    spdlog::get("logger")->info() << script;

    auto vs = executeScript(channelName, topic, script);

    timer("Topic => " + topic["name"].asString() + " completed.  Elapsed => ", t1);

    return vs;
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
    auto r = bulkClient.request("POST", "/_bulk", ss);
    stringstream o;
    o << r->content.rdbuf();

    //debug detail, else no need
    //spdlog::get("logger")->info() << o.str();

    Json::Value jv;
    o >> jv;
    spdlog::get("logger")->info() << "bulkToElastic() took " << jv["took"].asString() << "ms errors => " << jv["errors"].asString();

    ss.str("");
    ss << "Bulk to ES completed.  Total => " << v.size() << " Elapsed =>";
    timer(ss.str(), t1);

    return 0;
  };

  auto processChannel = [](unique_ptr<banana::channel>& channel_ptr)->int{
    
    vector<shared_ptr<string>> combined;

    auto pr = *channel_ptr;
    auto channel = pr.topics;
    
    for (int index = 0; index < channel.size(); ++index){      
      auto vs = processTopic(pr.name, channel[index]);
      combined.insert(combined.end(), vs.begin(), vs.end());
    }

    //When all is done, bulkload to elasticsearch
    if (combined.size() > 0){
      bulkToElastic(combined);
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

    parallel_for_each(v.begin(), v.end(), processChannel);
    
    return 0;
  };

};