#include "banana.h"
#include "spdlog/spdlog.h"

/*
LOGINREC *login;
DBPROCESS *dbproc;
RETCODE erc;
*/

int err_handler(DBPROCESS* dbproc, int severity, int dberr, int oserr, char* dberrstr, char* oserrstr) {
  if ((dbproc == NULL) || (DBDEAD(dbproc))) {
    spdlog::get("logger")->error() << "dbproc is NULL error: " << dberrstr;
    dbexit();
    return(INT_CANCEL);
  }
  else
  {
    spdlog::get("logger")->error() << "DB-Library error: " << dberrstr;

    if (oserr != DBNOERR){
      spdlog::get("logger")->error() << "Operating-system error: " << oserrstr;
    }
    
    dbclose(dbproc);
    dbexit();

    return(INT_CANCEL);
  }
}

int msg_handler(DBPROCESS* dbproc, DBINT msgno, int msgstate, int severity, char* msgtext, char* srvname, char* procname, int line) {
  /*
  ** If it's a database change message, we'll ignore it.
  ** Also ignore language change message.
  */
  if (msgno == 5701 || msgno == 5703)
    return(0);
  
  printf("Msg %d, Level %d, State %d\n", msgno, severity, msgstate);

  if (strlen(srvname) > 0)
    printf("Server '%s', ", srvname);
  if (strlen(procname) > 0)
    printf("Procedure '%s', ", procname);
  if (line > 0)
    printf("Line %d", line);

  printf("\n\t%s\n", msgtext);
  
  return(0);
}

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

  //handle server/network errors
  dberrhandle(err_handler);
  //dbmsghandle(msg_handler);

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
  if ((erc = dbuse(dbproc, db.c_str())) == FAIL) {
    spdlog::get("logger")->error() << "useDatabase() unable to use database " << db;
    return 1;
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
        char *buffer = pcol->status == -1 ? "NULL" : pcol->buffer;

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

  auto status = dbsqlexec(dbproc);

  if (status == FAIL) {
    spdlog::get("logger")->error() << "execute() dbsqlexec failed";
    return 1;
  }

  while ((erc = dbresults(dbproc)) != NO_MORE_RESULTS) {

    if (erc == FAIL) {
      spdlog::get("logger")->error() << "execute() no results";
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