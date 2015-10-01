//FreeTDSHelper.h
//author: huanghaifeng

#ifndef FREETDSHELPER_H
#define FREETDSHELPER_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
//#include <unistd.h>

#include <sybfront.h>	/* sybfront.h always comes first */
#include <sybdb.h>	/* sybdb.h is the only other file you need */

#include "spdlog/spdlog.h"

#define MAX_ROWS 1024
#define ERR_MSG_LEN 1024

struct FIELD
{
  char* data;
  int bufSize;
  FIELD(int size) :data(NULL), bufSize(size)
  {
    data = new char[size];
  }
  ~FIELD()
  {
    if (data != NULL)delete[] data;
    data = NULL;
  }
};

struct ROW
{
  int colcount;
  int* fieldLens;
  FIELD** fields;
  ROW(int* fLens, int columncount) :fields(NULL), colcount(columncount), fieldLens(fLens)
  {
    fields = new FIELD*[columncount];
    for (int i = 0; i<columncount; ++i)
    {
      fields[i] = new FIELD(fieldLens[i]);
    }
  }
  ~ROW()
  {
    if (fields != NULL)
    {
      for (int i = 0; i<colcount; ++i)
      {
        delete fields[i];
        fields[i] = NULL;
      }
      delete[] fields;
      fields = NULL;
    }
  }
  char* getField(int fieldNum)
  {
    return fields[fieldNum]->data;
  }
};

//sqlserver wrapper, not thread-safe
class FreeTDSHelper
{
public:
  FreeTDSHelper();
  ~FreeTDSHelper();

  bool openDB(char* server, char* user, char* password, char* dbname, char* charset = NULL);

  bool closeDB();

  bool execute(char* sql);

  int query(char* sql, int maxRowCount = MAX_ROWS);

  ROW* getRow(int rownum);

  bool releaseResultSet();

  int getRowCount() const;

  int getColumnCount() const;

  char* getErrorMessage();

private:
  FreeTDSHelper(const FreeTDSHelper&);
  FreeTDSHelper& operator=(const FreeTDSHelper&);

  LOGINREC* loginrec;
  DBPROCESS* dbprocess;

  int rowCount;
  int colCount;
  int* pBufSize;

  ROW* currentRow;
  bool prepareRowBuffer();
  bool destroyRowBuffer();

  char errMsg[ERR_MSG_LEN];
};

#endif	//FREETDSHELPER_H
