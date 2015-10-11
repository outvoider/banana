#include <iostream>
#include "banana.hpp"
#include "client_http.hpp"

typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;
static int err_handler(DBPROCESS*, int, int, int, char*, char*){};
static int msg_handler(DBPROCESS*, DBINT, int, int, char*, char*, char*, int){};

namespace {

  auto testSybdb = []()->int{

    LOGINREC *login;
    DBPROCESS *dbproc;
    RETCODE erc;

    //1
    if (dbinit() == FAIL) {
      fprintf(stderr, "%s:%d: dbinit() failed\n",
        "banana", __LINE__);
      exit(1);
    }
    //2
    //dberrhandle(err_handler);
    //dbmsghandle(msg_handler);

    // Get a LOGINREC.
    if ((login = dblogin()) == NULL) {
      fprintf(stderr, "%s:%d: unable to allocate login structure\n",
        "banana", __LINE__);
      exit(1);
    }
    
    DBSETLUSER(login, "hackclick");
    DBSETLPWD(login, "12345678");

    //5 connect to server
    if ((dbproc = dbopen(login, "localhost")) == NULL) {
      fprintf(stderr, "%s:%d: unable to connect to %s as %s\n",
        "banana", __LINE__,
        "SQLP91002\\DB02", "hackclick");
      exit(1);
    }

    //Use database
    if ("CRMS" && (erc = dbuse(dbproc, "CRMS")) == FAIL) {
      fprintf(stderr, "%s:%d: unable to use to database %s\n",
        "banana", __LINE__, "CRMS");
      exit(1);
    }

    //6 send a query
    dbcmd(dbproc, "SELECT 1, 'two', 'three'");

    // Send the command to SQL Server and start execution. 
    dbsqlexec(dbproc);

    //
    // Fetch Results
    //
    while ((erc = dbresults(dbproc)) != NO_MORE_RESULTS) {
      
      struct COL
      {
        char *name;
        char *buffer;
        int type, size, status;
      } *columns, *pcol;
      
      int ncols;
      int row_code;

      if (erc == FAIL) {
        fprintf(stderr, "%s:%d: dbresults failed\n",
          "banana", __LINE__);
        exit(1);
      }

      ncols = dbnumcols(dbproc);
      if ((columns = (COL*)calloc(ncols, sizeof(struct COL))) == NULL) {
        perror(NULL);
        exit(1);
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

        //printf("%*s ", pcol->size, pcol->name);

        cout << pcol->size << pcol->name;

        if ((pcol->buffer = (char*)calloc(1, pcol->size + 1)) == NULL){
          perror(NULL);
          exit(1);
        }

        erc = dbbind(dbproc, c, NTBSTRINGBIND,
          pcol->size + 1, (BYTE*)pcol->buffer);
        if (erc == FAIL) {
          fprintf(stderr, "%s:%d: dbbind(%d) failed\n",
            "banana", __LINE__, c);
          exit(1);
        }

        erc = dbnullbind(dbproc, c, &pcol->status);
          if (erc == FAIL) {
            fprintf(stderr, "%s:%d: dbnullbind(%d) failed\n",
              "banana", __LINE__, c);
            exit(1);
          }
      }
      printf("\n");

      /*
      * Print the data to stdout.
      */
      
      while ((row_code = dbnextrow(dbproc)) != NO_MORE_ROWS){
        
          switch (row_code) {
          case REG_ROW:
            for (pcol = columns; pcol - columns < ncols; pcol++) {
              char *buffer = pcol->status == -1 ?
                "NULL" : pcol->buffer;
              printf("%*s ", pcol->size, buffer);
            }
            printf("\n");
            break;

          case BUF_FULL:
            assert(row_code != BUF_FULL);
            break;

          case FAIL:
            fprintf(stderr, "%s:%d: dbresults failed\n",
              "banana", __LINE__);
            exit(1);
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
      */
      if (DBCOUNT(dbproc) > -1)
        fprintf(stderr, "%d rows affected\n", DBCOUNT(dbproc));

      /*
      * Check return status
      */
      if (dbhasretstat(dbproc) == TRUE) {
        printf("Procedure returned %d\n", dbretstatus(dbproc));
      }
    }

    dbclose(dbproc);
    dbexit();

    return 0;
  };

  auto testES = []()->int{
    auto esHost = globalConfig["es"][::env]["host"].asString();
    auto esPort = globalConfig["es"][::env]["port"].asString();

    //curl -XDELETE 'localhost:9200/customer?pretty'
    HttpClient c0(esHost + ":" + esPort);
    auto r0 = c0.request("DELETE", "/cdc?pretty");

    cout << "\n\n" << r0->content.rdbuf() << endl;

    string meta("{ \"index\" : { \"_index\" : \"cdc\", \"_type\" : \"test-type\", \"_id\" : \"1234\" } }");
    string body("{ \"foo\" : \"bar\", \"start_time\": \"2015-06-11T13:45:22\" }");
    stringstream ss;

    ss << meta << "\n" << body << "\n";

    HttpClient bulkClient(esHost + ":" + esPort);
    auto r = bulkClient.request("POST", "/_bulk", ss);
    cout << "\n\n" << r->content.rdbuf() << endl;

    HttpClient client(esHost + ":" + esPort);
    //auto r1 = client.request("GET", "/_nodes?settings=true&pretty=true");
    auto r1 = client.request("GET", "cdc/test_type/1234"); //or cdc/test_type/1234/_source
    cout << "\n\n" << r1->content.rdbuf() << endl;

    //curl -XGET "http://localhost:9200/myindex/_mapping"
    HttpClient client2(esHost + ":" + esPort);
    auto r2 = client2.request("GET", "cdc/_mapping");
    cout << "\n\n" << r2->content.rdbuf() << endl;

    return 0;
  };

  auto testESPostBulk = []()->int {

    auto esHost = globalConfig["es"][::env]["host"].asString();
    auto esPort = globalConfig["es"][::env]["port"].asString();

    //curl localhost : 9200 / index1, index2 / _stats
    HttpClient c0(esHost + ":" + esPort);
    auto r0 = c0.request("GET", "/cdc/_stats?pretty");
    cout << "\n\n" << r0->content.rdbuf() << endl;

    return 0;
  };

  void test() {
    testES();
    testESPostBulk();
  }

};