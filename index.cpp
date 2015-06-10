#include <iostream>
#include "test.cpp"
#include <boost/filesystem.hpp>
#include "spdlog/spdlog.h"

using namespace std;

auto setupLogging = []()->void {

  if (boost::filesystem::create_directory("./log")){
    boost::filesystem::path full_path(boost::filesystem::current_path());
    std::cout << "Successfully created directory"
      << full_path
      << "/log"
      << "\n";
  }

  size_t q_size = 1048576; //queue size must be power of 2
  spdlog::set_async_mode(q_size);

  //auto async_file = spdlog::daily_logger_st("logger", "log/presidential");
  //async_file->set_pattern("[%Y-%d-%m %H:%M:%S:%e] [%l] [thread %t] %v");

  std::vector<spdlog::sink_ptr> sinks;
  sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_st>());
  sinks.push_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>("log/banana", "txt", 0, 0));
  auto combined_logger = std::make_shared<spdlog::logger>("logger", begin(sinks), end(sinks));
  combined_logger->set_pattern("[%Y-%d-%m %H:%M:%S:%e] [%l] [thread %t] %v");
  spdlog::register_logger(combined_logger);

};

int main(int argc, char *argv[]) {

  setupLogging();

  //testJsonCons();

  //test config.json
  int res = testConfigFile();

  //test tdspp
  int res1 = testTdspp();

  return 0;
}