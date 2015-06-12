banana
======

###Main Tasks

Given config.json:

1. For each channel:
    1. For each topic:
        1. Executes the SQL statement and converts the each row to a 1-level inline JSON object.
    2. After all topics are processed:
        1. Bulkload to Elasticsearch using the Bulk API
        2. The index name is defined in config.json by the "es" key.
        3. The index type is defined by the "name" key of each topic.

###Dependencies

This project could not be possible without the following fabulous libraries
* [jsoncpp](https://github.com/open-source-parsers/jsoncpp) Open source JSON reader/writer
* [freetds](https://github.com/FreeTDS/freetds) Free implementation of Sybase's DB-Library, CT-Library
* [tdspp](https://github.com/htaox/tdspp) Wrapper for FreeTDS created by Voidsoft AB
* [spdlog](https://github.com/gabime/spdlog) Extremely fast logger
* [Simple-Web-Server](https://github.com/eidheim/Simple-Web-Server) Simple and fast HTTP/S server/client lib
* [boost](http://sourceforge.net/projects/boost/files/boost-binaries/) Well, you know

###Install
Compiled using VS2013, will definitely work with g++ with -std=c++11 flag.