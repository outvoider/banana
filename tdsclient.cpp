#include "banana.hpp"
#include "spdlog/spdlog.h"

/*
  ref: http://lists.ibiblio.org/pipermail/freetds/2004q3/016451.html

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
  
  //printf("Msg %d, Level %d, State %d\n", msgno, severity, msgstate);
  spdlog::get("logger")->warn() << "msgno " << msgno << " severity " << severity << " msgstate " << msgstate;

  if (strlen(srvname) > 0)
    spdlog::get("logger")->warn() << "Server => " << srvname;
  //printf("Server '%s', ", srvname);
  if (strlen(procname) > 0)
    spdlog::get("logger")->warn() << "Procedure => " << procname;
  //printf("Procedure '%s', ", procname);
  if (line > 0)
    spdlog::get("logger")->warn() << "line => " << line;
    //printf("Line %d", line);

  //printf("\n\t%s\n", msgtext);
  spdlog::get("logger")->warn() << "msgtext => " << msgtext;

  return(0);
}

banana::TDSClient::~TDSClient() {
  
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

  this->rows = make_unique<banana::TDSRows>();

  return 0;
};

int banana::TDSClient::connect() {

  int rc = this->init();

  if (rc){
    return rc;
  }

  //handle server/network errors
  dberrhandle(err_handler);
  dbmsghandle(msg_handler);

  // Get a LOGINREC for logging in
  if ((login = dblogin()) == NULL) {
    spdlog::get("logger")->error() << "connect() unable to allocate login structure";
    return 1;
  }

  DBSETLUSER(login, user.c_str());
  DBSETLPWD(login, pass.c_str());
  DBSETLAPP(login, "banana");

  this->dbproc = dbopen(login, host.c_str());

  // Frees the login record, can be called immediately after dbopen.
  dbloginfree(login);

  // Connect to server
  if (this->dbproc == NULL) {
    spdlog::get("logger")->error() << "connect() unable to connect to " << host;
    return 1;
  }
  return 0;

};

int banana::TDSClient::useDatabase(string& db){
  if ((erc = dbuse(dbproc, db.c_str())) == FAIL) {
    spdlog::get("logger")->error() << "useDatabase() unable to use database " << db;
    
    this->close();
    return 1;
  }
  spdlog::get("logger")->error() << "useDatabase() => " << db;
  return 0;
};

void banana::TDSClient::sql(string& _script){
  script = _script;
  dbcmd(dbproc, script.c_str());
};

int banana::TDSClient::getMetadata() {
  
  ncols = dbnumcols(dbproc);
  this->values.reserve(ncols);

  /*
  if ((columns = (COL*)calloc(ncols, sizeof(struct COL))) == NULL) {
    perror(NULL);
    return 1;
  }
  */
  
  /*
  * Read metadata and bind.
  */
  
  /*
  for (int c = 0; c < ncols; c++) {
    char* p = (char*)malloc(265);
    memset(p, 0, 256);
    buffers.push_back(p);
  }

  for (int c = 0; c < ncols; c++) {
    nullbind.push_back(1);
  }
  */

  //for (pcol = columns; pcol - columns < ncols; pcol++) {
  
  //for (int c = 0; c < ncols; c++) {
  for (int curCol = 0; curCol < ncols; ++curCol) {
    int c = curCol + 1;
    int type = dbcoltype(dbproc, c);
    int size = (SYBCHAR == type) ? dbcollen(dbproc, c) : 255;
    
    this->values.push_back(COL(dbcolname(dbproc, c), type, size));
    
    COL* pcol = &values[curCol];
    //int c = pcol - columns + 1;
    
    //pcol->name = dbcolname(dbproc, c);
    //pcol->type = dbcoltype(dbproc, c);
    //pcol->size = dbcollen(dbproc, c);
    
    //char* colname = dbcolname(dbproc, c+1);
    //int coltype = dbcoltype(dbproc, c+1);
    //int colsize = 255; // dbcollen(dbproc, c + 1);
    //int status;
    
    //if (SYBCHAR != pcol->type) {
    //  pcol->size = dbprcollen(dbproc, c);
    //  if (pcol->size > 255)
    //    pcol->size = 255;
    //}

    auto col = make_shared<TDSCell<string>>(pcol->name);
    
    //auto col = make_shared<TDSCell<string>>(colname);
    
    rows->fieldNames->push_back(col);

    //if ((pcol->buffer = (char*)calloc(1, pcol->size + 1)) == NULL){
    //  perror(NULL);
    //  return 1;
    //}

    //erc = dbbind(dbproc, c, NTBSTRINGBIND, pcol->size + 1, (BYTE*)pcol->buffer);
    erc = dbbind(dbproc, c, NTBSTRINGBIND, pcol->size() + 1, (BYTE*)&pcol->buffer[0]);

    //erc = dbbind(dbproc, c+1, NTBSTRINGBIND, colsize + 1, (BYTE*)buffers.at(c));
    if (erc == FAIL) {
      spdlog::get("logger")->error() << "dbbind " << c << " failed";
      return 1;
    }

    erc = dbnullbind(dbproc, c, &pcol->status);
    
    //erc = dbnullbind(dbproc, c+1, &nullbind.at(c));
    if (erc == FAIL) {
      spdlog::get("logger")->error() << "dbnullbind " << c << " failed";
      return 1;
    }
  }

  return 0;
};

int banana::TDSClient::fetchData() {
  
  while ((row_code = dbnextrow(dbproc)) != NO_MORE_ROWS){

    auto row = make_shared<banana::RowOfString>();

    switch (row_code) {
    case REG_ROW:
      //for (pcol = columns; pcol - columns < ncols; pcol++) {
      //for (auto& e : *rows->fieldNames){
      for (auto& e : this->values) {
        //char *buffer = pcol->status == -1 ? "NULL" : pcol->buffer;
        //printf("%*s ", pcol->size, buffer);
        //char* buffer = (nullbind.at(cidx) == -1 ? "NULL" : buffers.at(cidx));
        
        auto v = make_shared<banana::TDSCell<string>>(e.status == -1 ? "NULL" : e.buffer);
        row->push_back(v);
      }

      rows->fieldValues->push_back(row);
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
  //for (pcol = columns; pcol - columns < ncols; pcol++) {
  //  free(pcol->buffer);
  //}
  //free(columns);
  
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
      spdlog::get("logger")->info() << "execute() no results";
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
  
  this->close();
  //dbclose(dbproc);
  //dbexit();

  return 0;

};

void banana::TDSClient::close() {
  if (this->dbproc != NULL){
    dbclose(dbproc);
    this->dbproc = NULL;
  }
  dbexit();
}