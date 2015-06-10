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

#include "jsoncons/json.hpp"

using namespace std;

namespace {

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

  auto testConfigFile = []()->int{

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
      //read channels
      const Json::Value channels = globalConfig["channels"];
      for (Json::ValueIterator itr = channels.begin(); itr != channels.end(); itr++) {
        std::string name = itr.name();
        //cout << globalConfig["channels"][name] << "\n";
      }
      /*
      for (int index = 0; index < channels.size(); ++index){
      cout << channels[index];
      }
      */
      //cout << globalConfig;
    }
    else {
      return 1;
    }

    return 0;

  };

  auto testTdspp = []()->int{
    
    auto conn = globalConfig["connection"]["rn"]["staging"];
    //auto conn1 = gConfig["connection"]["rn"]["staging"];
    //
    auto db = unique_ptr<TDSPP>(new TDSPP());

    /* Connect to database. */
    try {
      //db->connect("clcwcdcdap001", "hackclick", "12345678");
      //db->connect(conn["host"].as_string(), conn["user"].as_string(), conn["pass"].as_string());
      db->connect(conn["host"].asString(), conn["user"].asString(), conn["pass"].asString());
    }
    catch (TDSPP::Exception &e){
      cerr << e.message << endl;
      return 1;
    }

    /* Execute command. */
    db->execute("use " + conn["database"].asString());
    
    //auto q = db->sql("select current_timestamp timestamp, UPPER('abc') as randomStuff");
    //auto q = db->sql("select count(1) from __IRBSubmission");

    /**
      channels
    **/
    stringstream scriptss;
    string script;
    string defaultLastExecTime = "GETDATE()";
    auto channels = globalConfig["channels"]["rn"];
    for (int index = 0; index < channels.size(); ++index){
      //cout << channels[index]["name"].asString();
      auto scriptArr = channels[index]["script"];
      for (auto& e : scriptArr){
        scriptss << e.asString();
      }
      std::regex e("\\$\\(LAST_EXEC_TIME\\)");
      script = scriptss.str();
      script = std::regex_replace(script, e, defaultLastExecTime);
    }
    //cout << script;
    
    spdlog::get("logger")->info() << script;
    
    auto q = db->sql(script);

    try {
      /* Execute SQL query. */
      q->execute();

      /* Print table headers, ie column names. */
      //Json::Value out;
      vector<string> vec;
      Rows::RowList rl = q->rows->rows;
      for (auto& r : rl){
        for (auto& v : r){
          //out[v->colname] = "";
          cout << v->colname << "\n";
          vec.push_back(v->colname);
        }
      }

      vector<shared_ptr<Json::Value>> vv;

      //q->rows->printheader();
      while (!q->eof()) {
        
        auto sv = shared_ptr<Json::Value>(new Json::Value);

        stringstream ss;
        for (int i = 0; i < q->fieldcount; i++){
          cout << vec.at(i) << "\n";
          //out[vec.at(i)] = q->fields(i)->tostr();
          Json::Value& jv = *sv;
          jv[vec.at(i)] = q->fields(i)->tostr();
          
          ss << q->fields(i)->tostr(); 
          if (i < q->fieldcount - 1){
            ss << " | ";
          }          
        }
        ss << "\n";
        spdlog::get("logger")->info() << ss.str();

        //cout << out;
        vv.push_back(sv);
        cout << *sv;

        q->next();
      }
    }
    catch (TDSPP::Exception &e) {
      cerr << e.message << endl;
    }

    /*x
    for (auto & r : q->rows){

    }
    */

    //db->disconnect();

    return 0;
  };

};