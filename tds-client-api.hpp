#include <iostream>
#include "tds-client.hpp"

namespace {

  const std::string appname = "banana";

  auto tdsClientExecute = [](std::string& servername, std::string& dbname, std::string& username, std::string& password, std::vector<std::string>& queries)->int{
    
    /* freetdsSample.c prefixes all error messages with argv[0]. */
    struct MyErrorHandler : public MSSQL::ErrorHandler {
      std::string appname;
      MyErrorHandler(std::string appname) : appname(appname) {   }
      virtual void fault(int line, std::string what) {
        std::stringstream ss; ss << appname << line << ": " << what;
        throw std::runtime_error(ss.str());
      }
      virtual void error(std::string what) { throw std::runtime_error(appname + ": " + what); }
      virtual void msg(std::string what) { std::cerr << appname << ": " << what; }
    };
    MyErrorHandler eh(appname);

    MSSQL::ClientConnection sql(&eh);

    try {
      sql.connect(servername, dbname, username, password);	/* (2) */
    }
    catch (std::runtime_error& e) {
      std::cerr << "failed to connect: " << e.what() << "\n";
      exit(-2);
    }

    // Execute all of the supplied queries and store result objects.
    std::vector<MSSQL::Result> results;
    std::vector<MyErrorHandler> errorHandlers;
    //for (int i = 0; i < argc; i++) {
    for (int i = 0; i < queries.size(); i++) {
    
      //for (const auto& query : queries) {
      //std::string query = argv[i];
      //std::string query = "select top 10 * from [__iacuc study];";
      //std::cout << "query " << i << ": " << query << "\n";
      std::string query = queries.at(i);

      errorHandlers.push_back(MyErrorHandler(query));
      results.push_back(sql.executeQuery(query, &errorHandlers[i]));
    }

    // Flag to say we've exhausted one or more result sets.
    bool lastRow = false;

    // Create result iterators for each result.
    std::vector<MSSQL::Result::iterator> resIters;
    for (size_t i = 0; i < results.size() && !lastRow; ++i) {
      MSSQL::Result::iterator resIter = results[i].begin();
      if (resIter.operator==(results[i].end()))
        lastRow = true; // empty result set
      else
        resIters.push_back(resIter);
    }

    // Walk through the rows and colums, displaying the results from each
    // query sequentially.
    while (!lastRow) {
      std::vector<MSSQL::Values::const_iterator> cols;
      for (size_t i = 0; i < resIters.size(); ++i)
        cols.push_back(resIters[i]->begin());
      for (bool lastCol = false; !lastCol;) {
        for (size_t i = 0; i < cols.size(); ++i) {
          std::cout << std::setw(cols[i]->size()) << cols[i]->lexical() << ' ';
          if (++cols[i] == resIters[i]->end())
            lastCol = true;
        }
        if (!lastCol)
          std::cout << '|';
      }
      std::cout << "\n";
      for (size_t i = 0; i < resIters.size(); ++i)
        if ((++resIters[i]).operator==(results[i].end()))
          lastRow = true;
      /*
      * Get row count, if available.
      */
      if (lastRow)
        for (size_t i = 0; i < resIters.size(); ++i)
          if (results[i].rowCount() > -1)
            std::cerr << "query " << i << ": " << results[i].rowCount() << " rows affected\n";

    }

    return 0;
  };

}
