#include <iostream>
#include <fstream>
#include <json/json.h>
#include "banana.h"
//#include <json/value.h>

#include <ctpublic.h>
#include "tdspp.hh"
#include <memory>
#include <regex>
#include "spdlog/spdlog.h"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "semaphore.hpp"
/*
#include "jsoncons/json.hpp"
*/

#include "client_http.hpp"
typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

using namespace std;

namespace {

  /*
  auto testJsonCons = []()->int{
    try{
      gConfig = jsoncons::json::parse_file("config.json");
      auto conn = gConfig["connection"]["rn"]["staging"];
      cout << conn["host"].as_string();    
    }
    catch (std::exception ex){
      cout << ex.what();
    }
    return 0;
  };
  **/

  auto testES = []()->int{
    auto esHost = globalConfig["es"]["dev"]["host"].asString();
    auto esPort = globalConfig["es"]["dev"]["port"].asString();
    
    //curl - XDELETE 'localhost:9200/customer?pretty'
    HttpClient c0(esHost + ":" + esPort);
    auto r0 = c0.request("DELETE", "/cdc?pretty");
    
    cout << "\n\n" << r0->content.rdbuf() << endl;

    string meta("{ \"index\" : { \"_index\" : \"cdc\", \"_type\" : \"test-type\", \"_id\" : \"1234\" } }");
    string body("{ \"foo\" : \"bar\", \"start_time\": \"2015-06-11T13:45:22\" }");
    stringstream ss;

    ss << meta << "\n" << body << "\n";

    HttpClient bulkClient(esHost + ":" + esPort);
    auto r = bulkClient.request("POST", "/_bulk", ss);
    cout << "\n\n"<< r->content.rdbuf() << endl;

    HttpClient client(esHost+":"+esPort);
    //auto r1 = client.request("GET", "/_nodes?settings=true&pretty=true");
    auto r1 = client.request("GET", "cdc/test_type/1234"); //or cdc/test_type/1234/_source
    cout << "\n\n" << r1->content.rdbuf() << endl;

    //curl -XGET "http://localhost:9200/myindex/_mapping"
    HttpClient client2(esHost + ":" + esPort);
    auto r2 = client2.request("GET", "cdc/_mapping"); 
    cout << "\n\n" << r2->content.rdbuf() << endl;

    return 0;
  };

  auto testESPostBulk = []()->int {

    auto esHost = globalConfig["es"]["dev"]["host"].asString();
    auto esPort = globalConfig["es"]["dev"]["port"].asString();

    //curl localhost : 9200 / index1, index2 / _stats
    HttpClient c0(esHost + ":" + esPort);
    auto r0 = c0.request("GET", "/cdc/_stats?pretty");
    cout << "\n\n" << r0->content.rdbuf() << endl;

    return 0;
  };

  auto loadConfigFile = []()->int{

    //Json::Value root;
    Json::Reader reader;
    ifstream ifs("config.json");

    if (ifs.is_open()){
      istream& s = ifs;
      bool parsingSuccessful = reader.parse(s, globalConfig);
      if (!parsingSuccessful){
        cout << "Failed to parse configuration\n"
          << reader.getFormattedErrorMessages();
        return 1;
      }
      //read channel
      const Json::Value channel = globalConfig["channel"];
      for (Json::ValueIterator itr = channel.begin(); itr != channel.end(); itr++) {
        std::string name = itr.name();
        //cout << globalConfig["channel"][name] << "\n";
      }      
    }
    else {
      return 1;
    }

    return 0;

  };

  auto executeScript = [](const string channelName, const Json::Value& topic, string& script)->vector<shared_ptr<string>>{

    vector<shared_ptr<string>> vs;

    auto conn = globalConfig["connection"][channelName]["staging"];
    cout << conn;
    auto db = unique_ptr<TDSPP>(new TDSPP());

    /* Connect to database. */
    try {
      db->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
    }
    catch (TDSPP::Exception &e){
      cerr << e.message << endl;
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
          //cout << v->colname << "\n";
          vec.push_back(v->colname);
        }
      }

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

        //.write will tack on a \n after completion
        Json::FastWriter writer;
        auto compactMeta = writer.write(meta);
        auto compactBody = writer.write(body);

        ss << compactMeta << compactBody;

        spdlog::get("logger")->info() << ss.str();

        //auto rv = *sv;
        //rv = ss.str();
        //*sv = ss.str();
        auto sv = shared_ptr<string>(new string(ss.str()));
        vs.push_back(sv);
        
        q->next();
      }

      cout << vs.size() << "\n";
    }
    catch (TDSPP::Exception &e) {
      spdlog::get("logger")->error() << e.message;
    }

    return vs;
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

  auto processTopic = [](string& channelName, Json::Value& topic)->vector<shared_ptr<string>>{

    //cout << "Processing " << topic["name"] << "\n";

    //start timer
    auto t1 = std::chrono::high_resolution_clock::now();

    stringstream scriptss;
    string script;
    //string defaultLastExecTime = "GETDATE()";
    string defaultLastExecTime = "CONVERT(datetime, '1970-01-01')";

    //cout << channel[index]["name"].asString();
    auto scriptArr = topic["script"];
    for (auto& e : scriptArr){
      scriptss << e.asString();
    }
    std::regex e("\\$\\(LAST_EXEC_TIME\\)");
    script = scriptss.str();
    script = std::regex_replace(script, e, defaultLastExecTime);
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

    auto esHost = globalConfig["es"]["dev"]["host"].asString();
    auto esPort = globalConfig["es"]["dev"]["port"].asString();

    //start timer
    auto t1 = std::chrono::high_resolution_clock::now();

    HttpClient bulkClient(esHost + ":" + esPort);
    auto r = bulkClient.request("POST", "/_bulk", ss);
    ostringstream o;
    o << r->content.rdbuf();
    spdlog::get("logger")->info() << o.str();

    string message = "Bulk to ES completed.  Elapsed => ";
    timer(message, t1);

    return 0;
  };

  auto processChannel = [](unique_ptr<banana::channel>& channel_ptr)->int{
    
    vector<shared_ptr<string>> combined;

    auto pr = *channel_ptr;
    auto channel = pr.topics;
    //need key

    cout << channel;

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
      //std::cout << id << std::endl;
      names.push_back(id);
    }

    //for (const auto& channel : channels) {
    for (auto i = 0; i < channels.size(); i++) {
      auto channel = channels[names[i]];
      string channelName = names[i];
      //cout << channel;
      //std::pair<string, Json::Value> pr = std::make_pair(channelName, new Json::Value(channel));
      //auto j = new Json::Value(channel);
      v.push_back(unique_ptr<banana::channel>(new banana::channel(channelName, channel)));
    }

    parallel_for_each(v.begin(), v.end(), processChannel);

    return 0;
  };

};