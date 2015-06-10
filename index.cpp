#include <iostream>
#include <fstream>
#include <json/json.h>
#include <json/value.h>

using namespace std;

int main(int argc, char *argv[]) {
  Json::Value root;
  Json::Reader reader;
  ifstream ifs("config.json");

  if (ifs.is_open()){
    istream& s = ifs;
    bool parsingSuccessful = reader.parse(s, root);
    if (!parsingSuccessful){
      cout << "Failed to parse configuration\n"
        << reader.getFormattedErrorMessages();
      return 1;
    }
    //read channels
    const Json::Value channels = root["channels"];
    for (Json::ValueIterator itr = channels.begin(); itr != channels.end(); itr++) {
      std::string name = itr.name();
      cout << root["channels"][name] << "\n";
    }
    /*
    for (int index = 0; index < channels.size(); ++index){
      cout << channels[index];
    }
    */
    cout << root;
  }  
  //
  return 0;
}