#include <iostream>
#include <chrono>
#include <thread>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "exports.cpp"
#include "test.cpp"

using namespace std;

auto setupLogging = []()->void {

  if (boost::filesystem::create_directory("./log")){
    boost::filesystem::path full_path(boost::filesystem::current_path());
    spdlog::get("logger")->info() << "Successfully created directory"
      << full_path
      << "/log"
      << "\n";
  }

  size_t q_size = 1048576; //queue size must be power of 2
  spdlog::set_async_mode(q_size);

  std::vector<spdlog::sink_ptr> sinks;
  //sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_st>());
  sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>("log/banana", "txt", 0, 0));
  auto combined_logger = std::make_shared<spdlog::logger>("logger", begin(sinks), end(sinks));
  combined_logger->set_pattern("[%Y-%d-%m %H:%M:%S:%e] [%l] [thread %t] %v");
  spdlog::register_logger(combined_logger);
  
};

auto createLmdb = [](string& dbPath)->void {
  int rc;
  rc = mdb_env_create(&lmdb_env);
  rc = mdb_env_set_mapsize(lmdb_env, 104857600);
  rc = mdb_env_set_maxdbs(lmdb_env, 4);
  //rc = mdb_env_open(lmdb_env, dbPath.c_str(), MDB_CREATE, 0664);
  rc = mdb_env_open(lmdb_env, dbPath.c_str(), MDB_CREATE, 0);
  if (rc != 0){
    spdlog::get("logger")->error() << "Failed to create lmdb.  Returned => " << rc;
    spdlog::get("logger")->flush();
    mdb_env_close(lmdb_env);
  }
  else {
    spdlog::get("logger")->info() << "Successfully created lmdb.";
  }
};

auto setupLmdbEnv = []()->void {
  boost::filesystem::path full_path(boost::filesystem::current_path());
  string dbPath = full_path.generic_string() + "/db";

  if (boost::filesystem::create_directory("./db")){
    //boost::filesystem::path full_path(boost::filesystem::current_path());
    spdlog::get("logger")->info() << "Successfully created directory"
      << full_path
      << "/db"
      << "\n";    
  }
  //createLmdb(dbPath);

};

int main(int argc, char *argv[]) {

  int res = loadConfigFile();
  if (res){
    return 1;
  }

  setupLogging();

  setupLmdbEnv();

  namespace po = boost::program_options;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "There is 1 parameter that you can pass")
    ("env", po::value<string>(), "dev, staging, prod");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    cout << desc << "\n";
    return 1;
  }

  // Loop through each argument and print its number and value
  spdlog::get("logger")->info("Banana has started. ");
  for (int nArg = 0; nArg < argc; nArg++) {
    spdlog::get("logger")->info() << " Parameter: " << nArg << " " << argv[nArg];
  }
  spdlog::get("logger")->flush();

  if (vm.count("env")) {
    ::env = (vm["env"].as<string>());
  }
  
  //get sleep_ms if defined
  if (globalConfig["sleep_ms"].isNumeric()){
    sleep_ms = globalConfig["sleep_ms"].asUInt();
  }

  //spin foreveer
  while (1){
    start();
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    spdlog::get("logger")->flush();
  }
  
  //will never get here anyway
  return 0;
}
