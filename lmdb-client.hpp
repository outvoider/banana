#include <iostream>
#include "lmdb.h"
#include <boost/filesystem.hpp>
#include "spdlog/spdlog.h"

namespace {

  class LMDBClient {
  public:
    LMDBClient() {
      this->lmdb_env = nullptr;
      boost::filesystem::path _full_path(boost::filesystem::current_path());
      this->full_path = _full_path.generic_string();
      this->dbPath = this->full_path + "/db";
      this->dbFile = this->full_path + "/db/data.mdb";

      setupLmdbEnv();
    }
    ~LMDBClient() {
      if (this->lmdb_env != nullptr){
        mdb_dbi_close(lmdb_env, lmdb_dbi);
        mdb_env_close(lmdb_env);
      }      
    }
    
    int setLmdbValue(std::string& k, std::string& v){

      if (lmdb_env == nullptr){
        spdlog::get("logger")->error() << "Error occurred mdb_put() => lmdb_env is nullptr";
        return -1;
      }
      int rc;
      MDB_txn *txn;
      MDB_val key, data;

      rc = mdb_txn_begin(lmdb_env, NULL, 0, &txn);
      rc = mdb_open(txn, NULL, MDB_CREATE, &lmdb_dbi);

      key.mv_size = k.size();
      key.mv_data = (char*)k.c_str();
      data.mv_size = v.size();
      data.mv_data = (char*)v.c_str();

      rc = mdb_put(txn, lmdb_dbi, &key, &data, 0);

      if (rc != 0){
        spdlog::get("logger")->error() << "Error occurred mdb_put() key => " << k << " value " << v << " rc => " << rc;
        mdb_txn_abort(txn);
        return rc;
      }

      //spdlog::get("logger")->info() << "key => " << k << " value => " << v;
      rc = mdb_txn_commit(txn);

      return 0;

    };

    std::string getLmdbValue(std::string& k) {

      if (lmdb_env == nullptr){
        spdlog::get("logger")->error() << "Error occurred mdb_get() => lmdb_env is nullptr";
        return "";
      }

      MDB_txn *txn;
      MDB_val key, data;

      int rc;

      rc = mdb_txn_begin(lmdb_env, NULL, 0, &txn);
      rc = mdb_open(txn, NULL, MDB_CREATE, &lmdb_dbi);

      key.mv_size = k.size();
      key.mv_data = (char*)k.c_str();

      rc = mdb_get(txn, lmdb_dbi, &key, &data);
      if (rc != 0){
        spdlog::get("logger")->error() << "Error occurred mdb_get() key => " << k << " rc => " << rc;
        mdb_txn_abort(txn);
        return "";
      }

      std::string res((const char*)data.mv_data, data.mv_size);

      rc = mdb_txn_commit(txn);
      
      return res;
    };

  private:
    MDB_env *lmdb_env;
    MDB_dbi lmdb_dbi;
    std::string dbPath;
    std::string full_path;
    std::string dbFile;

    /*
    think of this as "connect"
    */
    int setupLmdbEnv() {
      //create db folder if doesn't exist
      if (boost::filesystem::create_directory("./db")){
        spdlog::get("logger")->info() << "Successfully created directory" << this->full_path << "/db" << "\n";
      }
      
      //create lmdb
      int rc = createLmdb();
      
      //wait until os actually creats the db file
      waitForLmdbCreate();

      return rc;
    }

    void waitForLmdbCreate() {      
      while (1){
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        if (boost::filesystem::exists(dbFile)){
          break;
        }
      }
    }

    int createLmdb() {
      int rc;
      rc = mdb_env_create(&lmdb_env);
      rc = mdb_env_set_mapsize(lmdb_env, 10485760 * 10);
      rc = mdb_env_set_maxdbs(lmdb_env, 4);
      rc = mdb_env_open(lmdb_env, dbPath.c_str(), MDB_CREATE, 0664);
      
      if (rc != 0){
        char* c = mdb_strerror(rc);
        spdlog::get("logger")->error() << "Failed to create lmdb.  Returned => " << rc;
        spdlog::get("logger")->flush();
        mdb_env_close(lmdb_env);
        lmdb_env = nullptr;
        return rc;
      }
      
      //spdlog::get("logger")->info() << "Successfully created lmdb.";      
      return 0;
      
    };
  };

}