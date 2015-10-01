//FreeTDSHelper.cpp
//author: huanghaifeng

#include "FreeTDSHelper.h"

FreeTDSHelper::FreeTDSHelper()
  :loginrec(NULL), dbprocess(NULL), currentRow(NULL), rowCount(0), colCount(0), pBufSize(NULL)
{
  memset(errMsg, 0, ERR_MSG_LEN);
  dbinit();
}

FreeTDSHelper::~FreeTDSHelper()
{
  closeDB();
}

bool FreeTDSHelper::openDB(char* server, char* user, char* password, char* dbname, char* charset)
{
  if (server == NULL || dbname == NULL)
  {
    sprintf(errMsg, "Server-address or db-name you supplied is null, that is not allowed.");
    return false;
  }

  closeDB();

  loginrec = dblogin();
  DBSETLUSER(loginrec, user);
  DBSETLPWD(loginrec, password);
  DBSETLCHARSET(loginrec, charset == NULL ? "GBK" : charset);

  dbprocess = dbopen(loginrec, server);
  if (dbprocess == FAIL)
  {
    sprintf(errMsg, "dbopen error, can not connect to db-server.");
    dbloginfree(loginrec);
    loginrec = NULL;
    return false;
  }

  if (dbuse(dbprocess, dbname) == FAIL)
  {
    sprintf(errMsg, "dbuse error, can not use your chosen db.");
    dbclose(dbprocess);
    dbprocess = NULL;
    dbloginfree(loginrec);
    loginrec = NULL;
    return false;
  }
  return true;
}

bool FreeTDSHelper::closeDB() {

  if ((dbprocess != FAIL) || (dbprocess != NULL)) {
    if (currentRow != NULL) {
      releaseResultSet();
    }

    dbclose(dbprocess);
    dbprocess = NULL;
  }
  if (loginrec != NULL) {
    dbloginfree(loginrec);
    loginrec = NULL;
  }
  return true;
}

bool FreeTDSHelper::execute(char* sql)
{
  dbcmd(dbprocess, sql);
  if (dbsqlexec(dbprocess) == FAIL) {
    //snprintf(errMsg, ERR_MSG_LEN - 1, "dbsqlexec error(%s)", sql);
    return false;
  }
  return true;
}

int FreeTDSHelper::query(char * sql, int maxRowCount) {
  if (sql == NULL) {
    sprintf(errMsg, "Your sql string is null.");
    return -1;
  }

  char bufLen[16] = { 0 };
  sprintf(bufLen, "%d", maxRowCount);
  if (dbsetopt(dbprocess, DBBUFFER, bufLen, sizeof(bufLen)) == FAIL) {
    sprintf(errMsg, "dbsetopt error, can not set buffer.");
    return -1;
  }
  releaseResultSet();

  dbcmd(dbprocess, sql);
  if (dbsqlexec(dbprocess) == FAIL)
  {
    //snprintf(errMsg, ERR_MSG_LEN - 1, "dbsqlexec error(%s)", sql);
    spdlog::get("logger")->error() << "dbsqlexec error " << sql;
    return -1;
  }

  DBINT result_code;
  while ((result_code = dbresults(dbprocess)) != NO_MORE_RESULTS)
  {
    if (result_code == SUCCEED)
    {
      while (dbnextrow(dbprocess) != NO_MORE_ROWS)
      {
        ++rowCount;
        if (rowCount>MAX_ROWS)
        {
          //sprintf(errMsg, "Not enough buffer to hold all rows.");
          spdlog::get("logger")->error() << "Not enough buffer to hold all rows.";
          return -1;
        }
      }
    }
  }

  prepareRowBuffer();

  for (int i = 0; i<colCount; ++i)
  {
    if (dbbind(dbprocess, i + 1, CHARBIND, (DBCHAR)0, (BYTE*)(currentRow->getField(i))) == FAIL)
    {
      //sprintf(errMsg, "dbbind error, the column number given isn't valid, or /
      //  the vartype isn't compatible with the sqlserver data type being returned, or varaddr is null.");
      spdlog::get("logger")->error() << "dbbind error, the column number given isn't valid, or the vartype isn't compatible with the sqlserver data type being returned, or varaddr is null.";
      return -1;
    }
  }

  return rowCount;
}

ROW* FreeTDSHelper::getRow(int rownum)
{
  for (int i = 0; i<colCount; ++i)
  {
    memset(currentRow->getField(i), 0, pBufSize[i]);
  }

  dbgetrow(dbprocess, rownum + 1);
  return currentRow;
}

bool FreeTDSHelper::releaseResultSet()
{
  dbfreebuf(dbprocess);
  rowCount = 0;
  colCount = 0;
  destroyRowBuffer();
  return true;
}

bool FreeTDSHelper::prepareRowBuffer()
{
  colCount = dbnumcols(dbprocess);

  pBufSize = new int[colCount];

  for (int i = 1; i <= colCount; i++)
  {
    pBufSize[i - 1] = dbcollen(dbprocess, i);
  }

  currentRow = new ROW(pBufSize, colCount);
  return true;
}

bool FreeTDSHelper::destroyRowBuffer()
{
  if (currentRow != NULL)
  {
    delete currentRow;
    currentRow = NULL;
  }
  if (pBufSize != NULL)
  {
    delete[] pBufSize;
    pBufSize = NULL;
  }
  return true;
}

int FreeTDSHelper::getRowCount() const
{
  return rowCount;
}

int FreeTDSHelper::getColumnCount() const
{
  return colCount;
}

char* FreeTDSHelper::getErrorMessage()
{
  return errMsg;
}
