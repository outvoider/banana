#include "banana.h"
#include "spdlog/spdlog.h"

LOGINREC *login;
DBPROCESS *dbproc;
RETCODE erc;

int banana::TDSClient::connect(string& _host, string& _user, string& _pass){
  host = _host;
  user = _user;
  pass = _pass;
  return connect();
};

int banana::TDSClient::init() {
  if (dbinit() == FAIL) {
    spdlog::get("logger")->error() << "dbinit() failed";
    return 1;
  }
  return 0;
};

int banana::TDSClient::connect() {

  // Get a LOGINREC.
  if ((login = dblogin()) == NULL) {
    spdlog::get("logger")->error() << "connect() unable to allocate login structure";
    return 1;
  }

  DBSETLUSER(login, user.c_str());
  DBSETLPWD(login, pass.c_str());

  // Connect to server
  if ((dbproc = dbopen(login, host.c_str())) == NULL) {
    spdlog::get("logger")->error() << "connect() unable to connect to " << host;
    return 1;
  }
  return 0;

};

int banana::TDSClient::useDatabase(string& db){
  if ("CRMS" && (erc = dbuse(dbproc, "CRMS")) == FAIL) {
    spdlog::get("logger")->error() << "useDatabase() unable to use to database " << db;
    return 0;
  }
  return 0;
};

void banana::TDSClient::sql(string& _script){
  script = _script;
  dbcmd(dbproc, script.c_str());
};

int banana::TDSClient::getMetadata() {
  
  ncols = dbnumcols(dbproc);
  if ((columns = (COL*)calloc(ncols, sizeof(struct COL))) == NULL) {
    perror(NULL);
    return 1;
  }

  /*
  * Read metadata and bind.
  */
  
  for (pcol = columns; pcol - columns < ncols; pcol++) {
    int c = pcol - columns + 1;

    pcol->name = dbcolname(dbproc, c);
    pcol->type = dbcoltype(dbproc, c);
    pcol->size = dbcollen(dbproc, c);

    if (SYBCHAR != pcol->type) {
      pcol->size = dbprcollen(dbproc, c);
      if (pcol->size > 255)
        pcol->size = 255;
    }

    //cout << pcol->size << pcol->name;
    fieldNames.push_back(shared_ptr<string>(new string(pcol->name)));

    if ((pcol->buffer = (char*)calloc(1, pcol->size + 1)) == NULL){
      perror(NULL);
      return 1;
    }

    erc = dbbind(dbproc, c, NTBSTRINGBIND,
      pcol->size + 1, (BYTE*)pcol->buffer);
    if (erc == FAIL) {
      spdlog::get("logger")->error() << "dbnullbind " << c << " failed";
      return 1;
    }

    erc = dbnullbind(dbproc, c, &pcol->status);
    if (erc == FAIL) {
      spdlog::get("logger")->error() << "dbnullbind " << c << " failed";
      return 1;
    }
  }
  return 0;
};

int banana::TDSClient::fetchData() {

  while ((row_code = dbnextrow(dbproc)) != NO_MORE_ROWS){

    vector<shared_ptr<string>> row;

    switch (row_code) {
    case REG_ROW:
      for (pcol = columns; pcol - columns < ncols; pcol++) {
        char *buffer = pcol->status == -1 ?
          "NULL" : pcol->buffer;

        shared_ptr<string> v = shared_ptr<string>(new string(buffer));
        row.push_back(v);

        //printf("%*s ", pcol->size, buffer);
      }
      fieldValues.push_back(row);
      break;

    case BUF_FULL:
      assert(row_code != BUF_FULL);
      break;

    case FAIL:
      spdlog::get("logger")->error() << "dbresults failed";
      return 1;
      break;

    default:
      printf("Data for computeid %d ignored\n", row_code);
    }

  }

  /* free metadata and data buffers */
  for (pcol = columns; pcol - columns < ncols; pcol++) {
    free(pcol->buffer);
  }
  free(columns);

  /*
  * Get row count, if available.
    if (DBCOUNT(dbproc) > -1)
      fprintf(stderr, "%d rows affected\n", DBCOUNT(dbproc));
  */
  
  /*
  * Check return status
    if (dbhasretstat(dbproc) == TRUE) {
      printf("Procedure returned %d\n", dbretstatus(dbproc));
    }
  */
  return 0;
};

int banana::TDSClient::execute() {
  dbsqlexec(dbproc);

  while ((erc = dbresults(dbproc)) != NO_MORE_RESULTS) {

    if (erc == FAIL) {
      spdlog::get("logger")->error() << "execute() dbresults failed";      
      return 1;
    }

    /*
    * Read metadata and bind.
    */
    getMetadata();

    /*
      fetch data
    */
    fetchData();
  }

  dbclose(dbproc);
  dbexit();

  return 0;

};