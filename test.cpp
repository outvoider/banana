#include <iostream>
#include "banana.h"
#include "client_http.hpp"
typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

namespace {
  auto testES = []()->int{
    auto esHost = globalConfig["es"][::env]["host"].asString();
    auto esPort = globalConfig["es"][::env]["port"].asString();

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
    cout << "\n\n" << r->content.rdbuf() << endl;

    HttpClient client(esHost + ":" + esPort);
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

    auto esHost = globalConfig["es"][::env]["host"].asString();
    auto esPort = globalConfig["es"][::env]["port"].asString();

    //curl localhost : 9200 / index1, index2 / _stats
    HttpClient c0(esHost + ":" + esPort);
    auto r0 = c0.request("GET", "/cdc/_stats?pretty");
    cout << "\n\n" << r0->content.rdbuf() << endl;

    return 0;
  };

  void test() {
    testES();
    testESPostBulk();
  }

};