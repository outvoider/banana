#include <iostream>
#include <chrono>
#include <thread>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "exports.cpp"
#include "test.cpp"

#ifdef _DEBUG
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif  

using namespace std;

auto setupLogging = []()->void {

  if (boost::filesystem::create_directory("./log")){
    boost::filesystem::path full_path(boost::filesystem::current_path());
    cout << "Successfully created directory"
      << full_path
      << "/log"
      << "\n";
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  size_t q_size = 1048576; //queue size must be power of 2
  spdlog::set_async_mode(q_size);

  std::vector<spdlog::sink_ptr> sinks;
  //sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_st>());
  sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>("log/banana", "txt", 0, 0));
  auto combined_logger = std::make_shared<spdlog::logger>("logger", begin(sinks), end(sinks));
  combined_logger->set_pattern("[%Y-%d-%m %H:%M:%S:%e] [%l] [thread %t] %v");
  spdlog::register_logger(combined_logger);
  
};

int main(int argc, char *argv[]) {

#ifdef _DEBUG
  _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_DEBUG);
#endif

  int res = loadConfigFile();
  if (res){
    return 1;
  }

  setupLogging();

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

  //wait until lmdb is created
  //waitForLmdbCreate();
  
#ifdef _DEBUG
  _CrtMemState s1;
#endif

  //spin foreveer
  while (1){
    start();
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    spdlog::get("logger")->flush();
    
#ifdef _DEBUG
    _CrtMemCheckpoint(&s1);
    _CrtMemDumpStatistics(&s1);
    //_CrtDumpMemoryLeaks();
#endif
  }
  
  //will never get here anyway
  return 0;
}
