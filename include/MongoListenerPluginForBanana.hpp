#define STATIC_LIBMONGOCLIENT

#if defined(_WIN64)
#include <winsock2.h>
#include <windows.h>
#endif

#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <locale>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <ctime>
#include <chrono>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <future>
#include <regex>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "json11.hpp"

#include "mongo/client/dbclient.h"

#include "client_http.hpp"

#define _XOPEN_SOURCE 500
#include "build_compability_includes/real/win32compability.h"
#include "lmdb.h"

#include "spdlog/spdlog.h"

namespace MongoListenerPluginForBanana
{
	typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;

	using namespace std;
	using namespace std::chrono;
	using namespace json11;

	namespace Utility
	{
		bool HasMatch(string target, vector<string> patterns)
		{
			for (auto p : patterns)
			{
				if (regex_search(target, regex(p)))
				{
					return true;
				}
			}

			return false;
		}

		string AsIntegers(const char *text)
		{
			stringstream s;

			s << "[";

			if (*text != '\0')
			{
				s << (int)(*text);

				while (*++text != '\0')
				{
					s << ", " << (int)(*text);
				}
			}

			s << "]";

			return s.str();
		}

		void Retry(const string actionName
			, const vector<string> actionErrors
			, function<void()> NonReturningAction
			, const int attemptsCount = 10
			, const milliseconds pauseBetweenAttempts = milliseconds(1000))
		{
			for (int attempt = 0;;)
			{
				try
				{
					NonReturningAction();
					throw exception("a non returning action has returned");
				}
				catch (exception &ex)
				{
					++attempt;

					if (!HasMatch(ex.what(), actionErrors)
						|| (-1 != attemptsCount && attempt == attemptsCount))
					{
						spdlog::get("logger")->critical()
							<< actionName
							<< " has failed: an unsupported error has occured or retry() has run out of attempts, error message is '"
							<< ex.what()
							<< "', as integers "
							<< AsIntegers(ex.what());
						throw;
					}

					spdlog::get("logger")->error()
						<< actionName
						<< " has failed, attempt "
						<< attempt
						<< " out of "
						<< ((attemptsCount == -1) ? "infinity" : to_string(attemptsCount))
						<< ", next attempt in "
						<< pauseBetweenAttempts.count()
						<< " milliseconds, error message is '"
						<< ex.what()
						<< "', as integers "
						<< AsIntegers(ex.what());

					this_thread::sleep_for(pauseBetweenAttempts);
				}
				catch (...)
				{
					spdlog::get("logger")->critical()
						<< actionName
						<< " has failed: an unsupported and unspecified error has occured";
					throw;
				}
			}
		}
	}

	namespace Synchronized
	{
		template <typename T>
		class Buffer
		{
			list<T> a;
			list<T> b;
			list<T> *items;
			atomic_flag addLock;
			atomic_flag forLock;

		public:
			Buffer() : items(&a)
			{
				addLock.clear();
				forLock.clear();
			}

			void Add(const T &&item)
			{
				while (addLock.test_and_set());
				{
					items->emplace_back(item);
				}
				addLock.clear();
			}

			void For(function<void(const T &)> Action)
			{
				while (forLock.test_and_set());
				{
					auto *c = items;

					while (addLock.test_and_set());
					{
						items = (c == &a) ? &b : &a;
					}
					addLock.clear();

					for (auto &i : *c)
					{
						Action(i);
					}

					c->clear();
				}
				forLock.clear();
			}
		};

		template <typename T>
		class BufferDebug
		{
			list<T> a;
			list<T> b;
			list<T> *items;
			atomic_flag addLock;
			atomic_flag forLock;

		public:
			BufferDebug() : items(&a)
			{
				addLock.clear();
				forLock.clear();
			}

			void Add(const T &&item, function<void(const T &item)> OnAdd)
			{
				while (addLock.test_and_set());
				{
					OnAdd(item);
					items->emplace_back(item);
				}
				addLock.clear();
			}

			void For(function<void(const T &)> Action)
			{
				while (forLock.test_and_set());
				{
					auto *c = items;

					while (addLock.test_and_set());
					{
						items = (c == &a) ? &b : &a;
					}
					addLock.clear();

					for (auto &i : *c)
					{
						Action(i);
					}

					c->clear();
				}
				forLock.clear();
			}
		};
	}

	namespace MongoClient
	{
		mongo::DBClientBase* Create(const string &url)
		{
			string errmsg;
			auto cs = mongo::ConnectionString::parse(url, errmsg);

			if (!cs.isValid())
			{
				throw exception(errmsg.c_str());
			}

			mongo::DBClientBase *client = cs.connect(errmsg);

			if (client == nullptr)
			{
				throw exception(errmsg.c_str());
			}

			return client;
		}

		unsigned long long Count(const string &url
			, const string &database
			, const string &collection)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			return client->count(database + "." + collection);
		}

		void QueryCapped(function<void()> TryAbort

			, const string &url
			, const string &database
			, const string &collection
			, const mongo::BSONObj &lowerBound
			, function<void(const mongo::BSONObj&)> OnResult)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			auto query = MONGO_QUERY("_id" << mongo::GT << lowerBound["_id"]).sort("$natural");

			while (true)
			{
				auto cursor = client->query(database + "." + collection, query, 0, 0, 0, mongo::QueryOption_CursorTailable | mongo::QueryOption_AwaitData);

				if (!cursor.get())
				{
					throw exception("mongo querycapped has failed to obtain a cursor");
				}

				while (true)
				{
					TryAbort();

					if (!cursor->more())
					{
						if (cursor->isDead())
						{
							this_thread::sleep_for(chrono::milliseconds(1000));
							break;
						}

						continue;
					}

					auto document = cursor->nextSafe();
					query = MONGO_QUERY("_id" << mongo::GT << document["_id"]).sort("$natural");

					if (document.nFields() > 1)
					{
						OnResult(document);
					}
				}
			}
		}

		void Query(const string &url
			, const string &database
			, const string &collection
			, const mongo::BSONObj &lowerBound
			, function<void(const mongo::BSONObj&)> OnResult)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			auto query = MONGO_QUERY("_id" << mongo::GTE << lowerBound["_id"]).sort("_id");
			auto cursor = client->query(database + "." + collection, query);

			if (!cursor.get())
			{
				throw exception("mongo query has failed to obtain a cursor");
			}

			while (cursor->more())
			{
				auto document = cursor->nextSafe();

				if (document.nFields() > 1)
				{
					OnResult(document);
				}
			}
		}

		void Query(const string &url
			, const string &database
			, const string &collection
			, const mongo::BSONObj &lowerBound
			, const mongo::BSONObj &upperBound
			, function<void(const mongo::BSONObj&)> OnResult)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			auto query = MONGO_QUERY("_id" << mongo::GTE << lowerBound["_id"] << "_id" << mongo::LT << upperBound["_id"]).sort("_id");
			auto cursor = client->query(database + "." + collection, query);

			if (!cursor.get())
			{
				throw exception("mongo query has failed to obtain a cursor");
			}

			while (cursor->more())
			{
				auto document = cursor->nextSafe();

				if (document.nFields() > 1)
				{
					OnResult(document);
				}
			}
		}

		bool Contains(const string &url
			, const string &database
			, const string &collection
			, const mongo::BSONObj &document)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			auto query = MONGO_QUERY("_id" << mongo::GTE << document["_id"] << "_id" << mongo::LTE << document["_id"]).sort("_id");
			auto result = client->findOne(database + "." + collection, query);
			return !result.isEmpty();
		}

		void Insert(const mongo::BSONObj &document
			, const string &url
			, const string &database
			, const string &collection)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			client->insert(database + "." + collection, document);
			spdlog::get("logger")->info() << "mongo insert has finished";
		}

		void InsertPeriodic(unique_lock<mutex> &lock
			, const chrono::microseconds period

			, const string &url
			, const string &database
			, const string &collection
			, const string &cappedCollection

			, function<const mongo::BSONObj()> OnInsert)
		{
			condition_variable cv;
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));

			do
			{
				auto document = OnInsert();
				client->insert(database + "." + collection, document);
				client->insert(database + "." + cappedCollection, document);
			}
			while (cv.wait_for(lock, period) == cv_status::timeout);

			spdlog::get("logger")->info() << "mongo insertperiodic has finished";
		}

		void CreateCollection(const string &url
			, const string &database
			, const string &collection
			, const bool isCapped = true
			, const int maxDocumentsCount = 5000
			, const long long maxCollectionSize = 5242880)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			client->createCollection(database + "." + collection, maxCollectionSize, isCapped, maxDocumentsCount)
				? spdlog::get("logger")->info() << "mongo createcollection has finished"
				: spdlog::get("logger")->critical() << "mongo createcollection has failed";
		}

		void DropCollection(const string &url
			, const string &database
			, const string &collection)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			client->dropCollection(database + "." + collection)
				? spdlog::get("logger")->info() << "mongo dropcollection has finished"
				: spdlog::get("logger")->critical() << "mongo dropcollection has failed";
		}

		void DropDatabase(const string &url
			, const string &database)
		{
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			client->dropDatabase(database) ? spdlog::get("logger")->info() << "mongo dropdatabase has finished" : spdlog::get("logger")->critical() << "mongo dropdatabase has failed";
		}
	};

	namespace ElasticClient
	{
		Json Parse(string jsonString)
		{
			string error;
			auto json = Json::parse(jsonString, error);

			if (error != "")
			{
				throw exception(error.c_str());
			}

			return json;
		}

		void Index(const string json
			, const string &url
			, const string &index
			, const string &type
			, const string &id)
		{
			stringstream s; s << json << endl;
			HttpClient client(url);
			auto response = client.request("PUT", (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/" + id, s);
			s.str(""); s << response->content.rdbuf();
			spdlog::get("logger")->info() << s.str();
		}

		void Index(vector<Json>::const_iterator begin
			, vector<Json>::const_iterator end
			, const string &url
			, const string &index
			, const string &type
			, const int maxBatchCount = 10000
			, const int maxBatchSize = 100000000)
		{
			stringstream s;
			int n = 0;

			for (auto e = begin; e != end; ++e)
			{
				s << "{\"index\":{\"_id\":\"" << (*e)["_id"].string_value() << "\"}}" << endl;
				s << e->dump() << endl;

				if (++n == maxBatchCount || 1 + e == end || s.tellp() >= maxBatchSize)
				{
					HttpClient client(url);
					auto response = client.request("PUT", (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_bulk", s);
					s.str(""); s << response->content.rdbuf();
					spdlog::get("logger")->info() << s.str();
					s.str("");
					n = 0;
				}
			}
		}

		void Delete(const string &url
			, const string &index
			, const string &type)
		{
			stringstream s;
			HttpClient client(url);
			auto response = client.request("DELETE", (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)), s);
			s.str(""); s << response->content.rdbuf();
			spdlog::get("logger")->info() << s.str();
		}

		int Count(const string &url
			, const string &index
			, const string &type)
		{
			stringstream s;
			HttpClient client(url);
			s.str("");
			auto path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=0&size=0";
			auto response = client.request("GET", path, s);
			s.str(""); s << response->content.rdbuf();
			spdlog::get("logger")->info() << s.str();
			auto json = Parse(s.str());
			return json["hits"]["total"].int_value();
		}

		Json Search(const string &url
			, const string &index
			, const string &type
			, const string &query = ""
			, const int from = 0
			, const int count = 1000000
			, const int batchCount = 10000)
		{
			Json::array entries;
			stringstream s;
			HttpClient client(url);

			for (int i = from, n = min(count, batchCount); n != 0; i += n)
			{
				s.str(""); if (query != "") s << query << endl;
				auto path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=" + to_string(i) + "&size=" + to_string(n);
				auto response = client.request("GET", path, s);
				s.str(""); s << response->content.rdbuf();
				spdlog::get("logger")->info() << s.str();
				auto json = Parse(s.str());

				for (auto &e : json["hits"]["hits"].array_items())
				{
					entries.emplace_back(e);
				}

				n = min(batchCount, min(from + count, json["hits"]["total"].int_value()) - i);
			}

			return entries;
		}
	}

	namespace LmdbClient
	{
		string Get(const string &path, const string &key)
		{
			string value = "";
			int rc;
			MDB_env *env;
			MDB_dbi dbi;
			MDB_val key_p, data_p;
			MDB_txn *txn;

			rc = mdb_env_create(&env);
			rc |= mdb_env_open(env, path.c_str(), 0, 0664);
			rc |= mdb_txn_begin(env, NULL, 0, &txn);
			rc |= mdb_open(txn, NULL, 0, &dbi);

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);

			rc |= mdb_get(txn, dbi, &key_p, &data_p);

			if (rc == 0)
			{
				value.assign((const char*)data_p.mv_data, data_p.mv_size);
			}

			mdb_close(env, dbi);
			mdb_txn_abort(txn);
			mdb_env_close(env);

			spdlog::get("logger")->info() << (rc != 0 ? "lmdb get has failed" : "lmdb get has finished") << ", value is '" << value << "'";;

			return value;
		}

		void Set(const string &path, const string &key, const string &value)
		{
			int rc;
			MDB_env *env;
			MDB_dbi dbi;
			MDB_val key_p, data_p;
			MDB_txn *txn;

			rc = mdb_env_create(&env);
			rc |= mdb_env_open(env, path.c_str(), 0, 0664);
			rc |= mdb_txn_begin(env, NULL, 0, &txn);
			rc |= mdb_open(txn, NULL, 0, &dbi);

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);
			data_p.mv_data = (void*)value.data();
			data_p.mv_size = strlen((const char*)data_p.mv_data);

			rc |= mdb_put(txn, dbi, &key_p, &data_p, 0);
			rc |= mdb_txn_commit(txn);
			mdb_close(env, dbi);
			mdb_env_close(env);

			spdlog::get("logger")->info() << (rc != 0 ? "lmdb set has failed" : "lmdb set has finished") << ", value is '" << value << "'";
		}

		void Remove(const string &path, const string &key)
		{
			int rc;
			MDB_env *env;
			MDB_dbi dbi;
			MDB_val key_p, data_p;
			MDB_txn *txn;

			rc = mdb_env_create(&env);
			rc |= mdb_env_open(env, path.c_str(), 0, 0664);
			rc |= mdb_txn_begin(env, NULL, 0, &txn);
			rc |= mdb_open(txn, NULL, 0, &dbi);

			key_p.mv_data = (void*)key.data();
			key_p.mv_size = strlen((const char*)key_p.mv_data);

			rc |= mdb_del(txn, dbi, &key_p, NULL);

			if (rc == 0)
			{
				rc |= mdb_txn_commit(txn);
			}
			else
			{
				mdb_txn_abort(txn);
			}

			mdb_close(env, dbi);
			mdb_env_close(env);

			spdlog::get("logger")->info() << (rc != 0 ? "lmdb remove has failed" : "lmdb remove has finished");
		}
	}

	namespace BsonObject
	{
		unsigned long long ToMilliseconds(const mongo::BSONObj &document)
		{
			return document["_id"].OID().asDateT().millis;
		}

		const mongo::BSONObj FromMilliseconds(unsigned long long milliseconds)
		{
			mongo::Date_t d(milliseconds);
			mongo::OID oid; oid.init(d);
			return mongo::BSONObjBuilder().append("_id", oid).obj();
		}

		string ToModifiedJsonString(const mongo::BSONObj &document
			, const string &uidAttribute
			, const string &actionValue
			, const string &channelValue)
		{
			stringstream uid; uid << boost::uuids::random_generator()();

			mongo::BSONObjBuilder b; b
				.append("_id", uid.str())
				//.append("_id", document["_id"].OID().toString())
				//.append("action", actionValue)
				//.append("channel", channelValue)
				//.append("model", "mongo")
				.append("processed", 0)
				.append("start_time", mongo::Date_t(system_clock::now().time_since_epoch().count()));

			for (auto i = document.begin(); i.more();)
			{
				auto e = i.next();
				string n(e.fieldName());

				//if (n == uidAttribute)
				//{
				//	b.append("_uid", e.valuestrsafe());
				//}

				if (n != "_id")
				{
					b.append(e);
				}
			}

			return b.obj().jsonString();
		}
	}

	void Produce(const string &mongoUrl
		, const string &mongoDatabase
		, const string &mongoCollection
		, const string &mongoCappedCollection
		, const string &mongoUidAttribute)
	{
		stringstream uid;
		boost::uuids::random_generator rug;
		mutex m;
		unique_lock<mutex> lock(m);
		const chrono::microseconds period(1000);

		MongoClient::InsertPeriodic(lock, period, mongoUrl, mongoDatabase, mongoCollection, mongoCappedCollection, [&]()
		{
			uid.str("");
			uid << rug();
			auto time = system_clock::now().time_since_epoch().count();
			auto document = BSON(mongo::GENOID << mongoUidAttribute << uid.str() << "time" << time);

			cout << document.toString() << endl;

			return document;
		});
	}

	void Consume(const string &mongoUrl
		, const string &mongoDatabase
		, const string &mongoCollection
		, const string &mongoCappedCollection
		, const string &mongoUidAttribute

		, const string &elasticUrl
		, const string &elasticIndex
		, const string &elasticType
		, const string &elasticChannel

		, const string &lmdbPath
		, const string &lmdbMongoListenerLowerBound)
	{
		auto milliseconds = stoull("0" + LmdbClient::Get(lmdbPath, lmdbMongoListenerLowerBound));
		auto lowerBound = BsonObject::FromMilliseconds(milliseconds);
		atomic_flag hasFailed = ATOMIC_FLAG_INIT;
		auto hasMainQueryStarted = false;
		bool hasNewDocument = false;
		bool hasNewLowerBound = false;
		auto hasCompletedInitialQuery = false;
		bool hasProcessedInitialDocuments = false;
		exception failure;
		Synchronized::Buffer<mongo::BSONObj> buffer;

		auto tryAbort = [&]()
		{
			if (hasFailed._My_flag)
			{
				throw exception("abortion requested");
			}
		};

		auto handleError = [&](const string &actionName, function<void()> Action)
		{
			try
			{
				Action();
			}
			catch (exception &ex)
			{
				if (!hasFailed.test_and_set())
				{
					failure = exception(
						(actionName
						+ " has failed, error message is '"
						+ ex.what()
						+ "'").c_str());
				}
			}
			catch (...)
			{
				if (!hasFailed.test_and_set())
				{
					failure = exception(
						(actionName
						+ " has failed, error is unspecified").c_str());
				}
			}
		};

		auto handleUpdate = [&]()
		{
			while (true)
			{
				vector<Json> jsons;
				buffer.For([&](const mongo::BSONObj document)
				{
					auto jsonString = BsonObject::ToModifiedJsonString(document, mongoUidAttribute, elasticType, elasticChannel);
					auto json = ElasticClient::Parse(jsonString);
					jsons.emplace_back(json);

					auto m = BsonObject::ToMilliseconds(document);
					if (milliseconds < m)
					{
						milliseconds = m;
						hasNewLowerBound = true;
					}
					hasNewDocument = true;
				});

				if (hasNewDocument)
				{
					ElasticClient::Index(jsons.begin(), jsons.end(), elasticUrl, elasticIndex, elasticType);
					hasNewDocument = false;
				}
				else
				{
					this_thread::sleep_for(chrono::milliseconds(1));
				}

				if (hasProcessedInitialDocuments && hasNewLowerBound)
				{
					LmdbClient::Set(lmdbPath, lmdbMongoListenerLowerBound, to_string(milliseconds));
					hasNewLowerBound = false;
				}

				if (hasCompletedInitialQuery)
				{
					hasProcessedInitialDocuments = true;
				}

				tryAbort();
			}
		};

		auto handleInitQuery = [&]()
		{
			while (!hasMainQueryStarted && !hasFailed._My_flag)
			{
				this_thread::sleep_for(chrono::milliseconds(1));
			}

			MongoClient::Query(mongoUrl, mongoDatabase, mongoCollection, lowerBound, [&](const mongo::BSONObj &document)
			{
				buffer.Add(document.copy());
			});

			atomic_thread_fence(memory_order_seq_cst);
			hasCompletedInitialQuery = true;
		};

		auto handleMainQuery = [&]()
		{
			MongoClient::QueryCapped(tryAbort, mongoUrl, mongoDatabase, mongoCappedCollection, lowerBound, [&](const mongo::BSONObj &document)
			{
				hasMainQueryStarted = true;
				buffer.Add(document.copy());
			});
		};

		thread handleUpdateThread(handleError, "handleUpdate", handleUpdate);
		thread handleInitQueryThread(handleError, "handleInitQuery", handleInitQuery);
		handleError("handleMainQuery", handleMainQuery);

		handleUpdateThread.join();
		handleInitQueryThread.join();

		if (hasCompletedInitialQuery)
		{
			handleError("handleUpdate", handleUpdate);
			handleError("handleUpdate", handleUpdate);
		}

		throw failure;
	}

	void ConsumeDebug(const string &mongoUrl
		, const string &mongoDatabase
		, const string &mongoCollection
		, const string &mongoCappedCollection
		, const string &mongoUidAttribute

		, const string &elasticUrl
		, const string &elasticIndex
		, const string &elasticType
		, const string &elasticChannel

		, const string &lmdbPath
		, const string &lmdbMongoListenerLowerBound)
	{
		auto milliseconds = stoull("0" + LmdbClient::Get(lmdbPath, lmdbMongoListenerLowerBound));
		auto lowerBound = BsonObject::FromMilliseconds(milliseconds);
		atomic_flag hasFailed = ATOMIC_FLAG_INIT;
		auto hasMainQueryStarted = false;
		bool hasNewDocument = false;
		bool hasNewLowerBound = false;
		auto hasCompletedInitialQuery = false;
		bool hasProcessedInitialDocuments = false;
		exception failure;

		Synchronized::BufferDebug<mongo::BSONObj> buffer;

		ofstream init_file("../x64/Debug/_init_file.txt", ios_base::app);
		ofstream main_file("../x64/Debug/_main_file.txt", ios_base::app);
		ofstream buffer_file("../x64/Debug/_buffer_file.txt", ios_base::app);
		ofstream update_file("../x64/Debug/_update_file.txt", ios_base::app);
		ofstream oid_file("../x64/Debug/_oid_file.txt", ios_base::app);

		string oid = lowerBound["_id"].OID().toString();
		oid_file << oid << endl; oid_file.flush();

		auto tryAbort = [&]()
		{
			if (hasFailed._My_flag)
			{
				throw exception("abortion requested");
			}
		};

		auto handleError = [&](const string &actionName, function<void()> Action)
		{
			try
			{
				Action();
			}
			catch (exception &ex)
			{
				if (!hasFailed.test_and_set())
				{
					failure = exception(
						(actionName
						+ " has failed, error message is '"
						+ ex.what()
						+ "'").c_str());
				}
			}
			catch (...)
			{
				if (!hasFailed.test_and_set())
				{
					failure = exception(
						(actionName
						+ " has failed, error is unspecified").c_str());
				}
			}
		};

		auto handleUpdate = [&]()
		{
			while (true)
			{
				vector<Json> jsons;
				buffer.For([&](const mongo::BSONObj document)
				{
					auto jsonString = BsonObject::ToModifiedJsonString(document, mongoUidAttribute, elasticType, elasticChannel);
					auto json = ElasticClient::Parse(jsonString);
					jsons.emplace_back(json);

					auto m = BsonObject::ToMilliseconds(document);

					if (milliseconds < m)
					{
						milliseconds = m;
						hasNewLowerBound = true;
						oid = document["_id"].OID().toString();
					}

					hasNewDocument = true;
					update_file << document["_id"].OID().toString() << endl; update_file.flush();
				});

				if (hasNewDocument)
				{
					ElasticClient::Index(jsons.begin(), jsons.end(), elasticUrl, elasticIndex, elasticType);
					hasNewDocument = false;
				}
				else
				{
					this_thread::sleep_for(chrono::milliseconds(1));
				}

				if (hasProcessedInitialDocuments && hasNewLowerBound)
				{
					LmdbClient::Set(lmdbPath, lmdbMongoListenerLowerBound, to_string(milliseconds));
					hasNewLowerBound = false;
					oid_file << oid << endl; oid_file.flush();
				}

				if (hasCompletedInitialQuery)
				{
					hasProcessedInitialDocuments = true;
				}

				tryAbort();
			}
		};

		auto handleInitQuery = [&]()
		{
			while (!hasMainQueryStarted && !hasFailed._My_flag)
			{
				this_thread::sleep_for(chrono::milliseconds(1));
			}

			MongoClient::Query(mongoUrl, mongoDatabase, mongoCollection, lowerBound, [&](const mongo::BSONObj &document)
			{
				buffer.Add(document.copy(), [&](const mongo::BSONObj &item) { buffer_file << item["_id"].OID().toString() << endl; buffer_file.flush(); });
				init_file << document["_id"].OID().toString() << endl; init_file.flush();
			});

			atomic_thread_fence(memory_order_seq_cst);
			hasCompletedInitialQuery = true;
		};

		auto handleMainQuery = [&]()
		{
			MongoClient::QueryCapped(tryAbort, mongoUrl, mongoDatabase, mongoCappedCollection, lowerBound, [&](const mongo::BSONObj &document)
			{
				hasMainQueryStarted = true;
				buffer.Add(document.copy(), [&](const mongo::BSONObj &item) { buffer_file << item["_id"].OID().toString() << endl; buffer_file.flush(); });
				main_file << document["_id"].OID().toString() << endl; main_file.flush();
			});
		};

		thread handleUpdateThread(handleError, "handleUpdate", handleUpdate);
		thread handleInitQueryThread(handleError, "handleInitQuery", handleInitQuery);
		handleError("handleMainQuery", handleMainQuery);

		handleUpdateThread.join();
		handleInitQueryThread.join();

		if (hasCompletedInitialQuery)
		{
			handleError("handleUpdate", handleUpdate);
			handleError("handleUpdate", handleUpdate);
		}

		throw failure;
	}

	class Consumer
	{
		const string mongoUrl;
		const string mongoDatabase;
		const string mongoCollection;
		const string mongoCappedCollection;
		const string mongoUidAttribute;

		const string elasticUrl;
		const string elasticIndex;
		const string elasticType;
		const string elasticChannel;

		const string lmdbPath;
		const string lmdbMongoListenerLowerBound;

	public:
		Consumer(const string &mongoUrl
			, const string &mongoDatabase
			, const string &mongoCollection
			, const string &mongoCappedCollection
			, const string &mongoUidAttribute

			, const string &elasticUrl
			, const string &elasticIndex
			, const string &elasticType
			, const string &elasticChannel

			, const string &lmdbPath
			, const string &lmdbMongoListenerLowerBound)

			: mongoUrl(mongoUrl)
			, mongoDatabase(mongoDatabase)
			, mongoCollection(mongoCollection)
			, mongoCappedCollection(mongoCappedCollection)
			, mongoUidAttribute(mongoUidAttribute)

			, elasticUrl(elasticUrl)
			, elasticIndex(elasticIndex)
			, elasticType(elasticType)
			, elasticChannel(elasticChannel)

			, lmdbPath(lmdbPath)
			, lmdbMongoListenerLowerBound(lmdbMongoListenerLowerBound)
		{
		}

		void Consume(const int attemptsCount = -1
			, const milliseconds pauseBetweenAttempts = milliseconds(10000))
		{
			Utility::Retry("consume"
				, vector<string>({
				"connect: An operation on a socket could not be performed because the system lacked sufficient buffer space or because a queue was full"
				, "couldn't connect to server [^,]*, connection attempt failed"
				, "nextSafe\\(\\): \\{ \\$err: \\\"Executor error: CappedPositionLost: CollectionScan died due to position in capped collection being deleted\\. Last seen record id: RecordId\\([^)]*\\)\\\", code: 17144 \\}"
				, "nextSafe\\(\\): \\{ \\$err: \\\"getMore executor error: CappedPositionLost: CollectionScan died due to failure to restore tailable cursor position\\. Last seen record id: RecordId\\([^)]*\\)\\\", code: 17406 \\}"
				, "nextSafe\\(\\): \\{ \\$err: \\\"error processing query: ns=[^.]*\\.cappedTree: _id \\$gt ObjectId\\('[^']*'\\)\\nSort: \\{ \\$natural: 1 \\}\\nProj: \\{\\}\\ntailable cursor requested on \\.\\.\\.\\\", code: 2 \\}'" })
				, [&]()
				{
					MongoListenerPluginForBanana::Consume(mongoUrl, mongoDatabase, mongoCollection, mongoCappedCollection, mongoUidAttribute,
						elasticUrl, elasticIndex, elasticType, elasticChannel,
						lmdbPath, lmdbMongoListenerLowerBound);
				}
					, attemptsCount
					, pauseBetweenAttempts);
		}
	};

	void ExecutePeriodicUpsert()
	{
		// mongo::client::GlobalInstance must be instantiated in a function
		mongo::client::GlobalInstance mongoInstance;
		mongoInstance.assertInitialized();

		mutex m;
		unique_lock<mutex> lock(m);
		const chrono::seconds period(60);

		const string mongoUrl = "mongodb://192.168.230.131:27017";
		const string mongoDatabase = "test";
		const string mongoCollection = "persistent";
		const string mongoCappedCollection = "capped";
		const string mongoUidAttribute = "uid";

		const string elasticUrl = "192.168.230.131:9200";
		const string elasticIndex = "test";
		const string elasticType = "data";
		const string elasticChannel = "ldap:nyumc";

		const string lmdbPath = "C:/Outvoider/Projects/Nyulmc";
		const string lmdbMongoListenerLowerBound = "lmdbMongoListenerLowerBound";

		spdlog::daily_logger_mt("logger", "C:/Outvoider/Projects/logs/mongo-listener_log", 0, 0, true);
		//spdlog::daily_logger_mt("logger", "C:/Outvoider/Projects/logs/mongo-listener-producer_log", 0, 0, true);

		//MongoClient::CreateCollection(mongoUrl, mongoDatabase, mongoCappedCollection, true, 5);
		//MongoClient::Insert(mongo::BSONObj(), mongoUrl, mongoDatabase, mongoCappedCollection);

		//MongoClient::DropDatabase(mongoUrl, mongoDatabase);
		//ElasticClient::Delete(elasticUrl, elasticIndex, elasticType);
		//LmdbClient::Remove(lmdbPath, lmdbMongoListenerLowerBound);

		//Produce(mongoUrl, mongoDatabase, mongoCollection, mongoCappedCollection, mongoUidAttribute);

		//Consume(mongoUrl, mongoDatabase, mongoCollection, mongoCappedCollection, mongoUidAttribute,
		//	elasticUrl, elasticIndex, elasticType, elasticChannel,
		//	lmdbPath, lmdbMongoListenerLowerBound);

		Consumer(mongoUrl, mongoDatabase, mongoCollection, mongoCappedCollection, mongoUidAttribute,
			elasticUrl, elasticIndex, elasticType, elasticChannel,
			lmdbPath, lmdbMongoListenerLowerBound).Consume();

		//cout << MongoClient::Count(mongoUrl, mongoDatabase, mongoCollection) << endl;

		//MongoClient::Query(mongoUrl, mongoDatabase, mongoCollection, BsonObject::FromMilliseconds(0), [&](const mongo::BSONObj &document)
		//{
		//	cout << document.toString() << endl;
		//	//cout << document["_id"].OID().toString() << endl;
		//});

		//cout << ElasticClient::Count(elasticUrl, elasticIndex, elasticType) << endl;

		//auto json = ElasticClient::Search(elasticUrl, elasticIndex, elasticType);
		//for (auto e : json.array_items())
		//{
		//	cout << e.dump() << endl;
		//	//cout << e["_id"].string_value() << endl;
		//}
	}

	namespace Test
	{
		void Regex()
		{
			string target = "nextSafe(): { $err: \"Executor error: CappedPositionLost: CollectionScan died due to position in capped collection being deleted. Last seen record id: RecordId(94079)\", code: 17144 }";
			string pattern = "nextSafe\\(\\): \\{ \\$err: \\\"Executor error: CappedPositionLost: CollectionScan died due to position in capped collection being deleted\\. Last seen record id: RecordId\\([^)]*\\)\\\", code: 17144 \\}";

			if (regex_search(target, regex(pattern)))
			{
				cout << "success" << endl;
			}
		}

		void Buffer()
		{
			Synchronized::Buffer<int> a;

			a.Add(1);
			a.Add(2);
			a.Add(3);

			a.For([](int n)
			{
				cout << n << endl;
			});

			cout << "--------" << endl;

			a.Add(4);
			a.Add(5);
			a.Add(6);

			a.For([](int n)
			{
				cout << n << endl;
			});

			cout << "--------" << endl;
		}

		void AddFile(ifstream &f, unordered_set<string> &us, unsigned long long ub)
		{
			string s;

			while (getline(f, s))
			{
				auto ts = mongo::OID(s).asDateT().millis;

				if (ts < ub)
				{
					us.insert(s);
				}
			}
		}

		set<string> ReadToSet(string path)
		{
			set<string> set;
			ifstream input(path);
			string s;

			while (getline(input, s))
			{
				set.insert(s);
			}

			return set;
		}

		int Count(set<string> &set, unsigned long long lowerBound, unsigned long long upperBound)
		{
			auto result = 0;

			for (auto s : set)
			{
				auto time = mongo::OID(s).asDateT().millis;

				if (time >= lowerBound && time < upperBound)
				{
					++result;
				}
			}

			return result;
		}

		int Count(set<string> &set, unsigned long long lowerBound)
		{
			auto result = 0;

			for (auto s : set)
			{
				auto time = mongo::OID(s).asDateT().millis;

				if (time >= lowerBound)
				{
					++result;
				}
			}

			return result;
		}

		void Compare()
		{
			auto mongo_set = ReadToSet("../x64/Debug/listener-mongo-ids.txt");
			auto init_set = ReadToSet("../x64/Debug/_init_file.txt");
			auto main_set = ReadToSet("../x64/Debug/_main_file.txt");
			auto buffer_set = ReadToSet("../x64/Debug/_buffer_file.txt");
			auto update_set = ReadToSet("../x64/Debug/_update_file.txt");
			auto elastic_set = ReadToSet("../x64/Debug/listener-elastic-ids.txt");
			auto oid_set = ReadToSet("../x64/Debug/_oid_file.txt");

			//auto mongo_set = ReadToSet("listener-mongo-ids.txt");
			//auto init_set = ReadToSet("_init_file.txt");
			//auto main_set = ReadToSet("_main_file.txt");
			//auto buffer_set = ReadToSet("_buffer_file.txt");
			//auto update_set = ReadToSet("_update_file.txt");
			//auto elastic_set = ReadToSet("listener-elastic-ids.txt");
			//auto oid_set = ReadToSet("_oid_file.txt");

			auto all_set(init_set); all_set.insert(main_set.cbegin(), main_set.cend());

			cout << "oid: " << oid_set.size() << endl;
			cout << "----------------------" << endl;

			auto i = 0;
			auto lowerBound = 0ULL;
			auto upperBound = 0ULL;

			for (auto oid : oid_set)
			{
				lowerBound = upperBound;
				upperBound = mongo::OID(oid).asDateT().millis;

				cout << "mongo: " << Count(mongo_set, lowerBound, upperBound) << endl;
				cout << "all: " << Count(all_set, lowerBound, upperBound) << endl;
				cout << "buffer: " << Count(buffer_set, lowerBound, upperBound) << endl;
				cout << "update: " << Count(update_set, lowerBound, upperBound) << endl;
				cout << "elastic: " << Count(elastic_set, lowerBound, upperBound) << endl;
				cout << ++i << "-------------------" << endl;
			}

			lowerBound = 0;

			cout << "mongo: " << Count(mongo_set, lowerBound, upperBound) << endl;
			cout << "all: " << Count(all_set, lowerBound, upperBound) << endl;
			cout << "buffer: " << Count(buffer_set, lowerBound, upperBound) << endl;
			cout << "update: " << Count(update_set, lowerBound, upperBound) << endl;
			cout << "elastic: " << Count(elastic_set, lowerBound, upperBound) << endl;
			cout << "longest-prefix--------" << endl;

			lowerBound = upperBound;

			cout << "mongo: " << Count(mongo_set, lowerBound) << endl;
			cout << "all: " << Count(all_set, lowerBound) << endl;
			cout << "buffer: " << Count(buffer_set, lowerBound) << endl;
			cout << "update: " << Count(update_set, lowerBound) << endl;
			cout << "elastic: " << Count(elastic_set, lowerBound) << endl;
			cout << "shortest-suffix-------" << endl;

			cout << "mongo: " << mongo_set.size() << endl;
			cout << "all: " << all_set.size() << endl;
			cout << "buffer: " << buffer_set.size() << endl;
			cout << "update: " << update_set.size() << endl;
			cout << "elastic: " << elastic_set.size() << endl;
			cout << "total-----------------" << endl;
		}
	}

	void Execute()
	{
		//Test::Regex();
		//Test::Buffer();
		//Test::Compare();
		ExecutePeriodicUpsert();
	}

	//-------------------------------------------------------------------
	// TODO
	//
	//-------------------------------------------------------------------
}