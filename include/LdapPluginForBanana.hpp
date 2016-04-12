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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "LDAPConnection.h"
#include "LDAPConstraints.h"
#include "LDAPSearchReference.h"
#include "LDAPSearchResult.h"
#include "LDAPSearchResults.h"
#include "LDAPAttribute.h"
#include "LDAPAttributeList.h"
#include "LDAPEntry.h"
#include "LDAPException.h"
#include "LDAPModification.h"
#include "LDAPSchema.h"
#include "debug.h"

#include "json11.hpp"

#include "mongo/client/dbclient.h"

#include "client_http.hpp"

#define _XOPEN_SOURCE 500
#include "build_compability_includes/real/win32compability.h"
#include "lmdb.h"

#include "spdlog/spdlog.h"

namespace LdapPluginForBanana
{
	typedef SimpleWeb::Client<SimpleWeb::HTTP> HttpClient;
	typedef json11::Json Json;

	using namespace std;
	using namespace std::chrono;
	using namespace json11;

	namespace LdapDateTime
	{
		string FromTimeT(const time_t t)
		{
			stringstream s;
			auto gt = localtime(&t);

			s << gt->tm_year + 1900
				<< setfill('0') << setw(2) << gt->tm_mon + 1
				<< setfill('0') << setw(2) << gt->tm_mday
				<< setfill('0') << setw(2) << gt->tm_hour
				<< setfill('0') << setw(2) << gt->tm_min
				<< setfill('0') << setw(2) << gt->tm_sec
				<< ".0Z";

			return s.str();
		}

		time_t ToTimeT(const string &dt)
		{
			tm t;

			t.tm_isdst = -1;
			t.tm_year = stoi(dt.substr(0, 4)) - 1900;
			t.tm_mon = stoi(dt.substr(4, 2)) - 1;
			t.tm_mday = stoi(dt.substr(6, 2));
			t.tm_hour = stoi(dt.substr(8, 2));
			t.tm_min = stoi(dt.substr(10, 2));
			t.tm_sec = stoi(dt.substr(12, 2));

			return mktime(&t);
		}

		string ToUtc(const string &dt, const bool hasT = false, const bool hasM = false, const bool hasZ = false)
		{
			stringstream s;

			s << dt.substr(0, 4) << "-"
				<< dt.substr(4, 2) << "-"
				<< dt.substr(6, 2) << (hasT ? "T" : " ")
				<< dt.substr(8, 2) << ":"
				<< dt.substr(10, 2) << ":"
				<< dt.substr(12, 2);

			if (hasM)
			{
				s << ".00";
			}

			if (hasZ)
			{
				s << "Z";
			}

			return s.str();
		}
	}

	namespace LdapClient
	{
		//int SearchPaged(
		//	const string &url
		//	, const int port
		//	, const string &login
		//	, const string &password
		//	, const string &node
		//	, const string &filter
		//	, function<void(const LDAPEntry*)> OnEntry)
		//{
		//	auto cons = new LDAPConstraints;
		//	auto ctrls = new LDAPControlSet;
		//	ctrls->add(LDAPCtrl(LDAP_CONTROL_PAGEDRESULTS, true));
		//	cons->setServerControls(ctrls);
		//
		//	int result = LDAPResult::SUCCESS;
		//	LDAPAsynConnection *lc = nullptr;
		//
		//	try
		//	{
		//		lc = new LDAPAsynConnection(url, port, cons);
		//		cerr << "connected to " << url << ":" << port << endl;
		//	}
		//	catch (LDAPException e)
		//	{
		//		cerr << "failed to connect to " << url << ":" << port << endl;
		//		cerr << e << endl;
		//		throw;
		//	}
		//
		//	LDAPMessageQueue *q = nullptr;
		//
		//	try
		//	{
		//		q = lc->bind(login, password, cons);
		//		q->getNext();
		//		delete q;
		//		q = nullptr;
		//		cerr << "bound to " << lc->getHost() << endl;
		//	}
		//	catch (LDAPException e)
		//	{
		//		cerr << "failed to bind to " << lc->getHost() << endl;
		//		cerr << e << endl;
		//		throw;
		//	}
		//
		//	try
		//	{
		//		q = lc->search(
		//			node
		//			//, LDAPAsynConnection::SEARCH_BASE
		//			, LDAPAsynConnection::SEARCH_ONE
		//			//, LDAPAsynConnection::SEARCH_SUB
		//			, filter);
		//
		//		auto res = q->getNext();
		//		auto cont = true;
		//
		//		while (cont)
		//		{
		//			auto mt = res->getMessageType();
		//
		//			switch (mt)
		//			{
		//				case LDAP_RES_SEARCH_ENTRY:
		//					OnEntry(((LDAPSearchResult*)res)->getEntry());
		//					delete res;
		//					res = q->getNext();
		//					break;
		//				case LDAP_RES_SEARCH_REFERENCE:
		//					std::cerr << "reference: " << std::endl;
		//					delete res;
		//					res = q->getNext();
		//					break;
		//				default:
		//					result = ((LDAPResult*)res)->getResultCode();
		//					std::cerr << (*(LDAPResult*)res).resToString() << std::endl;
		//					
		//					auto ctrls = res->getSrvControls();
		//
		//					if (!ctrls.empty())
		//					{
		//
		//					}
		//					
		//					delete res;
		//					cerr << "loaded all ldap entries" << endl;
		//					cont = false;
		//					break;
		//			}
		//		}
		//
		//		delete q;
		//		lc->unbind();
		//		delete lc;
		//	}
		//	catch (LDAPException e)
		//	{
		//		delete q;
		//		lc->unbind();
		//		delete lc;
		//		cerr << "failed to load ldap entries" << endl;
		//		cerr << e << endl;
		//		throw;
		//	}
		//
		//	return result;
		//}

		int SearchSome(
			const string &url
			, const int port
			, const string &login
			, const string &password
			, const string &node
			, const string &filter
			, function<void(const LDAPEntry*)> OnEntry)
		{
			int result = LDAPResult::SUCCESS;
			LDAPAsynConnection *lc = nullptr;

			try
			{
				lc = new LDAPAsynConnection(url, port);
				spdlog::get("logger")->info() << "ldap searchsome has connected";
			}
			catch (const LDAPException &e)
			{
				spdlog::get("logger")->critical() << "ldap searchsome has failed to connect";
				spdlog::get("logger")->critical() << e;
				throw;
			}

			LDAPMessageQueue *q = nullptr;

			try
			{
				q = lc->bind(login, password);
				q->getNext();
				delete q;
				q = nullptr;
				spdlog::get("logger")->info() << "ldap searchsome has bound";
			}
			catch (const LDAPException &e)
			{
				spdlog::get("logger")->critical() << "ldap searchsome has failed to bind";
				spdlog::get("logger")->critical() << e;
				throw;
			}

			try
			{
				q = lc->search(
					node
					//, LDAPAsynConnection::SEARCH_BASE
					//, LDAPAsynConnection::SEARCH_ONE
					, LDAPAsynConnection::SEARCH_SUB
					, filter);

				auto res = q->getNext();
				auto cont = true;

				while (cont)
				{
					auto mt = res->getMessageType();

					switch (mt)
					{
						case LDAP_RES_SEARCH_ENTRY:
							OnEntry(((LDAPSearchResult*)res)->getEntry());
							delete res;
							res = q->getNext();
							break;
						case LDAP_RES_SEARCH_REFERENCE:
							spdlog::get("logger")->info() << "ldap searchsome reference: " << (*(LDAPResult*)res).resToString();
							delete res;
							res = q->getNext();
							break;
						default:
							result = ((LDAPResult*)res)->getResultCode();
							spdlog::get("logger")->info() << "ldap searchsome result: " << (*(LDAPResult*)res).resToString();
							delete res;
							cont = false;
							break;
					}
				}

				delete q;
				lc->unbind();
				delete lc;
			}
			catch (const LDAPException &e)
			{
				delete q;
				lc->unbind();
				delete lc;
				spdlog::get("logger")->critical() << "ldap searchsome has failed";
				spdlog::get("logger")->critical() << e;
				throw;
			}

			return result;
		}

		void Search(
			const string &url
			, const int port
			, const string &login
			, const string &password
			, const string &node
			, const string &filter
			, const string &idAttribute
			, const string &timeAttribute
			, const time_t lowerBound
			, const time_t upperBound
			, time_t &maxTime
			, function<void(const LDAPEntry*)> OnEntry)
		{
			unordered_set<string> ids;
			vector<time_t> times;
			stack<pair<time_t, time_t>> intervals; intervals.push({ lowerBound, upperBound });
			stringstream s;

			while (intervals.size() > 0)
			{
				auto i = intervals.top(); intervals.pop();

				assert(i.first < 0 || i.second < 0 || i.first <= i.second);

				auto lower = i.first >= 0 ? LdapDateTime::FromTimeT(i.first) : "";
				auto upper = i.second >= 0 ? LdapDateTime::FromTimeT(i.second) : "";
				s.str("");
				s << "(&";
				s << filter;
				s << "(" << idAttribute << "=*)";
				s << "(" << timeAttribute << "=*)";
				if (i.first >= 0) s << "(" << timeAttribute << ">=" << lower << ")";
				if (i.second >= 0) s << "(" << timeAttribute << "<=" << upper << ")";
				s << ")";

				auto utcLower = i.first >= 0 ? LdapDateTime::ToUtc(lower) : "";
				auto utcUpper = i.second >= 0 ? LdapDateTime::ToUtc(upper) : "";
				spdlog::get("logger")->info() << "ldap search searching [" << utcLower << ", " << utcUpper << "]";

				times.clear();
				auto result = SearchSome(url, port, login, password, node, s.str(), [&](const LDAPEntry *entry)
				{
					auto al = entry->getAttributes();

					for (auto a = al->begin(); a != al->end(); ++a)
					{
						auto an = a->getName();

						if (an == idAttribute)
						{
							auto vl = a->getValues();
							auto id = vl.size() == 0 ? "" : *vl.begin();

							if (ids.insert(id).second)
							{
								OnEntry(entry);
							}
						}

						if (an == timeAttribute)
						{
							auto vl = a->getValues();
							auto t = LdapDateTime::ToTimeT(*vl.begin());

							if (maxTime < t)
							{
								maxTime = t;
							}

							times.push_back(t);
						}
					}
				});

				if (result == LDAPResult::SIZE_LIMIT_EXCEEDED || result == LDAPResult::TIME_LIMIT_EXCEEDED)
				{
					sort(times.begin(), times.end());

					for (auto t = times.rbegin() + times.size() / 2; t != times.rend(); ++t)
					{
						if (*t != times.back())
						{
							intervals.push({ i.first, *t });
							intervals.push({ *t, i.second });
							result = LDAPResult::SUCCESS;
							break;
						}
					}
				}

				if (result != LDAPResult::SUCCESS)
				{
					spdlog::get("logger")->error() << "ldap search has failed searching [" << utcLower << ", " << utcUpper << "]";
				}
			}
		}
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

		void Upsert(vector<mongo::BSONObj>::const_iterator begin
			, vector<mongo::BSONObj>::const_iterator end
			, const string &url
			, const string &database
			, const string &collection)
		{
			if (begin != end)
			{
				boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
				auto builder = client->initializeUnorderedBulkOp(database + "." + collection);

				for (auto o = begin; o != end; ++o)
				{
					auto value = (*o)["_id"].str();
					builder.find(BSON("_id" << value)).upsert().replaceOne(*o);
				}

				mongo::WriteResult result;
				builder.execute(0, &result);
			}

			spdlog::get("logger")->info() << "mongo upsert has finished";
		}

		vector<mongo::BSONObj> Query(const string &url
			, const string &database
			, const string &collection)
		{
			vector<mongo::BSONObj> objects;
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			auto cursor = client->query(database + "." + collection, mongo::BSONObj());

			if (!cursor.get())
			{
				throw exception("mongo query has failed to obtain a cursor");
			}

			while (cursor->more())
			{
				objects.push_back(cursor->nextSafe().copy());
			}

			spdlog::get("logger")->info() << "mongo query has finished";

			return objects;
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

		void Delete(vector<string>::const_iterator begin
			, vector<string>::const_iterator end
			, const string &url
			, const string &index
			, const string &type)
		{
			stringstream s;

			if (begin != end)
			{
				s << "{\"ids\":[";

				for (auto i = begin;;)
				{
					s << "\"" << *i << "\"";

					if (++i == end)
					{
						break;
					}

					s << ",";
				}

				s << "]}";
			}

			HttpClient client(url);
			auto response = client.request("DELETE", (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)), s);
			s.str(""); s << response->content.rdbuf();
			spdlog::get("logger")->info() << s.str();
		}

		Json Get(vector<string>::const_iterator begin
			, vector<string>::const_iterator end
			, const string &url
			, const string &index
			, const string &type)
		{
			string error;
			stringstream s;

			if (begin != end)
			{
				s << "{\"ids\":[";

				for (auto i = begin;;)
				{
					s << "\"" << *i << "\"";

					if (++i == end)
					{
						break;
					}

					s << ",";
				}

				s << "]}";

				HttpClient client(url);
				auto response = client.request("GET", (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_mget", s);
				s.str(""); s << response->content.rdbuf();
				spdlog::get("logger")->info() << s.str();
			}

			return Json::parse(s.str(), error);
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

	namespace LdapEntry
	{
		class ToJson
		{
			enum AttributeTypes { Default, DateTime, Binary, Variant };

			unordered_map<string, AttributeTypes> types;

		public:
			ToJson()
			{
				types = unordered_map<string, AttributeTypes>
				{
					{ "msExchWhenMailboxCreated", AttributeTypes::DateTime }
					, { "msTSExpireDate", AttributeTypes::DateTime }
						, { "whenChanged", AttributeTypes::DateTime }
						, { "whenCreated", AttributeTypes::DateTime }
						, { "dSCorePropagationData", AttributeTypes::DateTime }

						, { "msExchMailboxGuid", AttributeTypes::Binary }
						, { "msExchMailboxSecurityDescriptor", AttributeTypes::Binary }
						, { "objectGUID", AttributeTypes::Binary }
						, { "objectSid", AttributeTypes::Binary }
						, { "userParameters", AttributeTypes::Binary }
						, { "userCertificate", AttributeTypes::Binary }
						, { "msExchArchiveGUID", AttributeTypes::Binary }
						, { "msExchBlockedSendersHash", AttributeTypes::Binary }
						, { "msExchSafeSendersHash", AttributeTypes::Binary }
						, { "securityProtocol", AttributeTypes::Binary }
						, { "terminalServer", AttributeTypes::Binary }
						, { "mSMQDigests", AttributeTypes::Binary }
						, { "mSMQSignCertificates", AttributeTypes::Binary }
						, { "msExchSafeRecipientsHash", AttributeTypes::Binary }
						, { "msExchDisabledArchiveGUID", AttributeTypes::Binary }
						, { "sIDHistory", AttributeTypes::Binary }
						, { "replicationSignature", AttributeTypes::Binary }
						, { "msExchMasterAccountSid", AttributeTypes::Binary }
						, { "logonHours", AttributeTypes::Binary }
						, { "thumbnailPhoto", AttributeTypes::Binary }

						, { "extensionAttribute1", AttributeTypes::Variant }
						, { "extensionAttribute2", AttributeTypes::Variant }
						, { "extensionAttribute3", AttributeTypes::Variant }
						, { "extensionAttribute4", AttributeTypes::Variant }
						, { "extensionAttribute5", AttributeTypes::Variant }
						, { "extensionAttribute6", AttributeTypes::Variant }
						, { "extensionAttribute7", AttributeTypes::Variant }
						, { "extensionAttribute8", AttributeTypes::Variant }
						, { "extensionAttribute9", AttributeTypes::Variant }
						, { "extensionAttribute10", AttributeTypes::Variant }
						, { "extensionAttribute11", AttributeTypes::Variant }
						, { "extensionAttribute12", AttributeTypes::Variant }
						, { "extensionAttribute13", AttributeTypes::Variant }
						, { "extensionAttribute14", AttributeTypes::Variant }
						, { "extensionAttribute15", AttributeTypes::Variant }
				};
			}

			Json operator()(const LDAPEntry *entry
				, const string &idAttribute
				, const string &actionValue
				, const string &channelValue)
			{
				Json::object jMappings;
				auto al = entry->getAttributes();

				for (auto a = al->begin(); a != al->end(); ++a)
				{
					Json::array jValues;
					auto an = a->getName();
					auto vl = a->getValues();

					if (types.find(an) == types.end())
					{
						//case AttributeTypes::Default:
						{
							for (auto &v : vl)
							{
								jValues.push_back(v);
							}
						}
					}
					else switch (types[an])
					{
						case AttributeTypes::DateTime:
						{
							for (auto &v : vl)
							{
								jValues.push_back(LdapDateTime::ToUtc(v, true));
							}

							break;
						}

						case AttributeTypes::Binary:
						{
							for (auto &v : vl)
							{
								jValues.push_back("");
							}

							break;
						}

						case AttributeTypes::Variant:
						{
							for (auto &v : vl)
							{
								jValues.push_back("[string] " + v);
							}

							break;
						}
					}

					jMappings.insert({
						an == idAttribute ? "_uid" : an,
						(0 == vl.size()) ? nullptr : (1 == vl.size()) ? jValues[0] : jValues
					});
				}

				stringstream uid; uid << boost::uuids::random_generator()();
				jMappings.insert({ "_id", uid.str() });
				jMappings.insert({ "action", actionValue });
				jMappings.insert({ "channel", channelValue });
				jMappings.insert({ "model", "ldap" });
				jMappings.insert({ "processed", 0 });
				jMappings.insert({ "start_time", jMappings[jMappings.find("whenChanged") != jMappings.end() ? "whenChanged" : "whenCreated"] });

				return jMappings;
			}
		};

		class ToBson
		{
			enum AttributeTypes { Default, DateTime, };

			unordered_map<string, AttributeTypes> types;

		public:
			ToBson()
			{
				types = unordered_map<string, AttributeTypes>
				{
					{ "msExchWhenMailboxCreated", AttributeTypes::DateTime }
					, { "msTSExpireDate", AttributeTypes::DateTime }
						, { "whenChanged", AttributeTypes::DateTime }
						, { "whenCreated", AttributeTypes::DateTime }
						, { "dSCorePropagationData", AttributeTypes::DateTime }
				};
			}

			mongo::BSONObj operator()(const LDAPEntry *entry, const string &idAttribute)
			{
				mongo::BSONObjBuilder oBuilder;
				auto al = entry->getAttributes();

				for (auto a = al->begin(); a != al->end(); ++a)
				{
					mongo::BSONArrayBuilder aBuilder;
					auto an = a->getName();
					auto vl = a->getValues();
					auto id = an == idAttribute ? "_id" : an;

					if (0 == vl.size())
					{
						oBuilder.appendNull(id);
					}
					else if (1 == vl.size())
					{
						if (types.find(an) == types.end())
						{
							//case AttributeTypes::Default:
							{
								oBuilder.append(id, *vl.begin());
							}
						}
						else switch (types[an])
						{
							case AttributeTypes::DateTime:
							{
								oBuilder.appendTimeT(id, LdapDateTime::ToTimeT(*vl.begin()));
								break;
							}
						}
					}
					else
					{
						if (types.find(an) == types.end())
						{
							//case AttributeTypes::Default:
							{
								for (auto v = vl.begin(); v != vl.end(); ++v)
								{
									aBuilder.append(*v);
								}
							}
						}
						else switch (types[an])
						{
							case AttributeTypes::DateTime:
							{
								for (auto v = vl.begin(); v != vl.end(); ++v)
								{
									aBuilder.appendTimeT(LdapDateTime::ToTimeT(*vl.begin()));
								}

								break;
							}
						}

						oBuilder.append(id, aBuilder.arr());
					}
				}

				return oBuilder.obj();
			}
		};
	}

	namespace LmeClient
	{
		void Upsert(
			const string &ldapUrl
			, const int ldapPort
			, const string &ldapLogin
			, const string &ldapPassword
			, const string &ldapNode
			, const string &ldapFilter
			, const string &ldapIdAttribute
			, const string &ldapTimeAttribute
			, const time_t ldapLowerBound
			, const time_t ldapUpperBound
			, time_t &maxTime

			, const string &mongoUrl
			, const string &mongoDatabase
			, const string &mongoCollection

			, const string &elasticUrl
			, const string &elasticIndex
			, const string &elasticType
			, const string &elasticChannel

			, const string &fileName)
		{
			LdapEntry::ToJson toJson;
			vector<Json> jEntries;
			LdapEntry::ToBson toBson;
			vector<mongo::BSONObj> bEntries;
			ofstream backup(fileName);

			LdapClient::Search(ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapTimeAttribute, ldapLowerBound, ldapUpperBound, maxTime, [&](const LDAPEntry *entry)
			{
				jEntries.push_back(toJson(entry, ldapIdAttribute, elasticType, elasticChannel));
				bEntries.push_back(toBson(entry, ldapIdAttribute));

				backup << jEntries.back().dump() << endl;
				backup.flush();
			});

			backup.close();
			ElasticClient::Index(jEntries.begin(), jEntries.end(), elasticUrl, elasticIndex, elasticType);
			MongoClient::Upsert(bEntries.begin(), bEntries.end(), mongoUrl, mongoDatabase, mongoCollection);

			//sort(jEntries.begin(), jEntries.end(), [&](const Json &left, const Json &right)
			//{
			//	return left["_id"].string_value() < right["_id"].string_value();
			//});

			//for (auto e = jEntries.begin(); e != jEntries.end(); ++e)
			//{
			//	cout << e->dump() << endl;
			//}
		}

		void UpsertPeriodic(unique_lock<mutex> &lock
			, const chrono::seconds period

			, const string &ldapUrl
			, const int ldapPort
			, const string &ldapLogin
			, const string &ldapPassword
			, const string &ldapNode
			, const string &ldapFilter
			, const string &ldapIdAttribute
			, const string &ldapCreateTimeAttribute
			, const string &ldapUpdateTimeAttribute

			, const string &mongoUrl
			, const string &mongoDatabase
			, const string &mongoCollection

			, const string &elasticUrl
			, const string &elasticIndex
			, const string &elasticType
			, const string &elasticChannel

			, const string &lmdbPath
			, const string &lmdbKeyForLdapLowerBound

			, const string &createdFileName
			, const string &updatedFileName)
		{
			//std::time_t t = system_clock::to_time_t(p);
			//chrono::system_clock::time_point tp = chrono::system_clock::from_time_t(tt);
			condition_variable cv;

			do
			{
				spdlog::get("logger")->info() << "lme upsertperiodic has begun";

				auto end = system_clock::now();
				auto value = LmdbClient::Get(lmdbPath, lmdbKeyForLdapLowerBound);
				time_t ldapLowerBound = value == "" ? -1 : LdapDateTime::ToTimeT(value);
				time_t maxTime = ldapLowerBound;

				LmeClient::Upsert(ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapCreateTimeAttribute, ldapLowerBound, -1, maxTime,
					mongoUrl, mongoDatabase, mongoCollection,
					elasticUrl, elasticIndex, elasticType, elasticChannel,
					createdFileName);

				LmeClient::Upsert(ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapUpdateTimeAttribute, ldapLowerBound, -1, maxTime,
					mongoUrl, mongoDatabase, mongoCollection,
					elasticUrl, elasticIndex, elasticType, elasticChannel,
					updatedFileName);

				ldapLowerBound = maxTime - (system_clock::to_time_t(system_clock::now()) - system_clock::to_time_t(end));
				LmdbClient::Set(lmdbPath, lmdbKeyForLdapLowerBound, LdapDateTime::FromTimeT(ldapLowerBound));

				spdlog::get("logger")->info() << "lme upsertperiodic has finished, next in " << period.count() << " seconds";
			}
			while (cv.wait_for(lock, period) == cv_status::timeout);
		}
	};

	namespace ElasticClientTest
	{
		void Delete()
		{
			vector<string> v;
			ElasticClient::Delete(v.begin(), v.end(), "192.168.230.131:9200", "a_index", "a_type");
		}

		void Get()
		{
			vector<string> v = { "aaa", "bbb" };
			ElasticClient::Get(v.begin(), v.end(), "192.168.230.131:9200", "a_index", "a_type");
		}

		void Search()
		{
			ElasticClient::Search("192.168.230.131:9200", "a_index", "a_type");
		}

		void Index()
		{
			string error;
			vector<Json> v = {
				Json::parse("{ \"_id\" : \"aaa\", \"f1\" : \"v12\", \"f2\" : \"v22\" }", error)
				, Json::parse("{ \"_id\" : \"bbb\", \"f1\" : \"v32\", \"f2\" : \"v42\" }", error) };
			ElasticClient::Index(v.begin(), v.end(), "192.168.230.131:9200", "a_index", "a_type");
		}

		Json Parse(string s, int n)
		{
			stringstream ss;
			int f = 0;
			bool b = false;
			Json::object o;
			string key;
			string value;
			Json::array v;

			for (auto c : s)
			{
				if (c != '\"')
				{
					ss << c;
				}
				else if (f == 3)
				{
					if (b)
					{
						f = 2;
						value = ss.str();
						ss.str("");
						v.push_back(value);
					}
					else
					{
						f = 0;
						value = ss.str();
						ss.str("");
						o.insert({ key, value });

						if (--n == 0)
						{
							break;
						}
					}

				}
				else if (f == 2)
				{
					f = 3;
					ss.str("");
				}
				else if (f == 1)
				{
					f = 2;
					key = ss.str();
					ss.str("");
				}
				else
				{
					f = 1;
					ss.str("");
				}

				if (f == 2 && c == '[')
				{
					b = true;
				}

				if (f == 2 && c == ']')
				{
					f = 0;
					b = false;

					o.insert({ key, v });

					if (--n == 0)
					{
						break;
					}

					v.clear();
				}
			}

			return o;
		}

		void PrintBinaryAttributes()
		{
			unordered_set<string> keys;
			string error;
			string buffer;
			ifstream input("users.txt");
			int n = 0;

			while (getline(input, buffer))
			{
				cerr << ++n << endl;
				auto j = Json::parse(buffer, error);

				for (auto &o : j.object_items())
				{
					if (o.second.is_string())
					{
						for (auto ch : o.second.string_value())
						{
							if (static_cast<uint8_t>(ch) <= 0x1f || static_cast<uint8_t>(ch) == 0xe2 || static_cast<uint8_t>(ch) == 0xe2)
							{
								if (keys.insert(o.first).second)
								{
									cout << o.first << endl;
								}

								break;
							}
						}
					}
					else if (o.second.is_array())
					{
						for (auto &p : o.second.array_items())
						{
							if (p.is_string())
							{
								for (auto ch : p.string_value())
								{
									if (static_cast<uint8_t>(ch) <= 0x1f || static_cast<uint8_t>(ch) == 0xe2 || static_cast<uint8_t>(ch) == 0xe2)
									{
										if (keys.insert(o.first).second)
										{
											cout << o.first << endl;
										}

										break;
									}
								}
							}
						}
					}
				}
			}
		}

		void StringifyAttributes()
		{
			unordered_set<string> attributes = {
				"extensionAttribute1",
				"extensionAttribute2",
				"extensionAttribute3",
				"extensionAttribute4",
				"extensionAttribute5",
				"extensionAttribute6",
				"extensionAttribute7",
				"extensionAttribute8",
				"extensionAttribute9",
				"extensionAttribute10",
				"extensionAttribute11",
				"extensionAttribute12",
				"extensionAttribute13",
				"extensionAttribute14",
				"extensionAttribute15" };
			string error;
			string buffer;
			ifstream input("users.txt");
			int n = 0;

			while (getline(input, buffer))
			{
				cerr << ++n << endl;
				auto j = Json::parse(buffer, error);
				Json::object e;

				for (auto &o : j.object_items())
				{
					e.insert({ o.first, (attributes.find(o.first) == attributes.end()) ? o.second : "[string] " + o.second.string_value() });
				}

				cout << Json(e).dump() << endl;
			}
		}

		void Test()
		{
			const string elasticUrl = "192.168.230.131:9200";
			const string elasticIndex = "nyu";
			const string elasticType = "mc_users";

			ElasticClient::Search(elasticUrl, elasticIndex, elasticType);

			string error;
			string buffer;
			ifstream input("users.txt");
			vector<Json> entries;
			vector<string> ids;
			int i = 0;

			ElasticClient::Delete(ids.begin(), ids.end(), elasticUrl, elasticIndex, elasticType);
			this_thread::sleep_for(chrono::milliseconds(1000));

			while (getline(input, buffer))
			{
				entries.emplace_back(Json::parse(buffer, error));

				if (++i % 30000 == 0)
				{
					cout << i << " ------------------------------------------------------------------------------" << endl;
					ElasticClient::Index(entries.begin(), entries.end(), elasticUrl, elasticIndex, elasticType);
					entries.clear();
				}
			}

			if (entries.size() != 0)
			{
				cout << i << " ------------------------------------------------------------------------------" << endl;
				ElasticClient::Index(entries.begin(), entries.end(), elasticUrl, elasticIndex, elasticType);
				entries.clear();
			}

			//ifstream input("input.txt");
			//string s((istreambuf_iterator<char>(input)), istreambuf_iterator<char>());
			//input.close();
			//ofstream output("output.txt");
			//output << s;
			//output.close();

			////	"{\"_id\": \"liuf01\", \"accountExpires\": \"9223372036854775807\", \"badPasswordTime\": \"130082696356265334\", \"badPwdCount\": \"0\", \"codePage\": \"0\", \"countryCode\": \"0\", \"dSCorePropagationData\": [\"20151202175349.0Z\", \"20150826214808.0Z\", \"20150826214612.0Z\", \"20150712023024.0Z\", \"16010101000417.0Z\"], \"department\": \"ChildandAdolescentPsychiatry\", \"description\": \"NYUMC\", \"displayName\": \"Liu, Feng\", \"distinguishedName\": \"CN = liuf01, OU = NYUMCUsers, DC = nyumc, DC = org\", \"employeeID\": \"1002627\", \"employeeType\": \"Faculty\", \"extensionAttribute12\": \"12 / 09 / 2014\", \"extensionAttribute13\": \"CWR\", \"extensionAttribute2\": \"SOMBU\", \"extensionAttribute3\": \"Yes\", \"extensionAttribute4\": \"09 / 17 / 1956\", \"extensionAttribute6\": \"4314\", \"extensionAttribute8\": \"A\", \"extensionAttribute9\": \"1\", \"gidNumber\": \"10000\", \"givenName\": \"Feng\", \"homeDirectory\": \"\\\\Homedir.nyumc.org\\Users\\home003\\liuf01\", \"homeDrive\": \"H:\", \"homeMDB\": \"CN = MBXDB2469, CN = Databases, CN = ExchangeAdministrativeGroup(FYDIBOHF23SPDLT), CN = AdministrativeGroups, CN = NYUMC, CN = MicrosoftExchange, CN = Services, CN = Configuration, DC = nyumc, DC = org\", \"homeMTA\": \"CN = MicrosoftMTA, CN = MSGWCDCPMB25, CN = Servers, CN = ExchangeAdministrativeGroup(FYDIBOHF23SPDLT), CN = AdministrativeGroups, CN = NYUMC, CN = MicrosoftExchange, CN = Services, CN = Configuration, DC = nyumc, DC = org\", \"instanceType\": \"4\", \"lastLogoff\": \"0\", \"lastLogon\": \"131007256904965954\", \"lastLogonTimestamp\": \"131019302232583801\", \"legacyExchangeDN\": \" / o = NYUMC / ou = FirstAdministrativeGroup / cn = Recipients / cn = liuf0183422839\", \"lockoutTime\": \"0\", \"logonCount\": \"186\", \"logonHours\": \"\", \"mDBUseDefaults\": \"TRUE\", \"mail\": \"Feng.Liu@nyumc.org\", \"mailNickname\": \"liuf01\", \"memberOf\": [\"CN = NYUEmployeeUsers, OU = Distribution, OU = NYULMCGroups, DC = nyumc, DC = org\", \"CN = SP - PORTAL_EMPLOYEE_LANG, OU = 2013PRODGroups, OU = SharepointGroups, OU = AdminGroups, OU = NYUMCInfrastructure, DC = nyumc, DC = org\", \"CN = Psoftnotification, CN = Users, DC = nyumc, DC = org\", \"CN = \\#WomensLeadershipEvent2015, OU = Distribution, OU = NYULMCGroups, DC = nyumc, DC = org\", \"CN = HCM_FACULTY_BELLEVUE, OU = SecurityGroups, OU = Exchange, DC = nyumc, DC = org\", \"CN = HCM_FACULTY_COMP_NONCOMP, OU = SecurityGroups, OU = Exchange, DC = nyumc, DC = org\", \"CN = HCM_RSH_PI, OU = SecurityGroups, OU = Exchange, DC = nyumc, DC = org\", \"CN = HCM_ALL_PERSONNEL, OU = SecurityGroups, OU = Exchange, DC = nyumc, DC = org\", \"CN = \\#KiDS2015RFP, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = \\#Non - TenureFaculty, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = \\#PSIUserstest, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = XEN - EPIC - PRD, OU = Applications, OU = PCD, OU = NYUMCInfrastructure, DC = nyumc, DC = org\", \"CN = AllFacultyUsers, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = \\#Bellevue_faculty_staff, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = UserLoginInfo, OU = Security, OU = Groups, OU = Hospital, DC = nyumc, DC = org\", \"CN = \\#NYUCTFBroadcast8, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = SchoolEmployees, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = \\#CSCSharePoint, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = EAVEnabledFaculty, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = \\#ActiveFaculty, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = CTX - APPS - EPIC - PRD - OSH, OU = Applications, OU = PCD, OU = NYUMCInfrastructure, DC = nyumc, DC = org\", \"CN = Juniper - OSH - IT - NetworkConnect, OU = VPNAccess, OU = Security, OU = NYULMCGroups, DC = nyumc, DC = org\", \"CN = \\#NYUCTFBroadcast, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = PeoplesoftGroup, OU = NYUMCUsers, DC = nyumc, DC = org\", \"CN = EavEnabledMbxMB06, OU = DistributionList, OU = Exchange, DC = nyumc, DC = org\", \"CN = CTX - apps - Office2007, OU = Applications, OU = PCD, OU = NYUMCInfrastructure, DC = nyumc, DC = org\"], \"msExchALObjectVersion\": \"89\", \"msExchAuditDelegate\": \"1533\", \"msExchAuditOwner\": \"1085\", \"msExchELCMailboxFlags\": \"22\", \"msExchHomeServerName\": \" / o = NYUMC / ou = ExchangeAdministrativeGroup(FYDIBOHF23SPDLT) / cn = Configuration / cn = Servers / cn = MSGWCDCPMB25\", \"msExchMailboxAuditEnable\": \"FALSE\", \"msExchMailboxAuditLastAdminAccess\": \"20160307054238.0Z\", \"msExchMailboxGuid\": \"O; DH }\", \"msExchMailboxSecurityDescriptor\": \"0L | Pt; +h@Pt; +h@0k`6$Pt; + LJ$Pt; +BL$Pt; +$Pt; +BL$Pt; +$Pt; +LJ$Pt; +K$Pt; +K$Pt; +$Pt; +&b$Pt; +&b$Pt; +>$Pt; +R:$Pt; +I$Pt; +R:$Pt; +$Pt; +&b$Pt; +$Pt; +$Pt; +$Pt; +v$Pt; +H$Pt; +$Pt; +$Pt; +$Pt; +D$Pt; +H$Pt; +V$Pt; +v$Pt; +w$Pt; +I$Pt; +H$Pt; +V$Pt; +H$Pt; +$Pt; +H$Pt; +$Pt; +$Pt; +D$Pt; +v$Pt; +z$Pt; +H$Pt; +6$Pt; +6$Pt; +$Pt; +$Pt; +$Pt; +$Pt; +$Pt; +\", \"msExchMailboxTemplateLink\": \"CN = PurgeDeletedItemsAfter90DaysPolicy, CN = RetentionPoliciesContainer, CN = NYUMC, CN = MicrosoftExchange, CN = Services, CN = Configuration, DC = nyumc, DC = org\", \"msExchMobileMailboxFlags\": \"1\", \"msExchOmaAdminWirelessEnable\": \"5\", \"msExchPoliciesIncluded\": [\"885464f1 - a556 - 4355 - 8fda - def2caee1dd8\", \"{26491cfc - 9e50 - 4857 - 861b - 0cb8df22b5d7}\"], \"msExchRBACPolicyLink\": \"CN = DefaultRoleAssignmentPolicy, CN = Policies, CN = RBAC, CN = NYUMC, CN = MicrosoftExchange, CN = Services, CN = Configuration, DC = nyumc, DC = org\", \"msExchRecipientDisplayType\": \"1073741824\", \"msExchRecipientTypeDetails\": \"1\", \"msExchUserAccountControl\": \"0\", \"msExchUserCulture\": \"en - US\", \"msExchVersion\": \"44220983382016\", \"msExchWhenMailboxCreated\": \"2012 - 03 - 07T21:06 : 42.00\", \"msNPAllowDialin\": \"FALSE\", \"msRTCSIP - DeploymentLocator\": \"SRV : \", \"msRTCSIP - FederationEnabled\": \"TRUE\", \"msRTCSIP - InternetAccessEnabled\": \"TRUE\", \"msRTCSIP - OptionFlags\": \"257\", \"msRTCSIP - PrimaryHomeServer\": \"CN = LcServices, CN = Microsoft, CN = 1 : 1, CN = Pools, CN = RTCService, CN = Services, CN = Configuration, DC = nyumc, DC = org\", \"msRTCSIP - PrimaryUserAddress\": \"sip : Feng.Liu@nyumc.org\", \"msRTCSIP - UserEnabled\": \"TRUE\", \"msRTCSIP - UserPolicies\": \"0 = 24473232\", \"msSFU30NisDomain\": \"nyumc\", \"name\": \"liuf01\", \"objectCategory\": \"CN = Person, CN = Schema, CN = Configuration, DC = nyumc, DC = org\", \"objectClass\": [\"top\", \"person\", \"organizationalPerson\", \"user\"], \"objectGUID\": \"n#M\", \"objectSid\": \"Pt; +Bo\", \"primaryGroupID\": \"513\", \"protocolSettings\": [\"HTTP11\", \"OWA1\"], \"proxyAddresses\": [\"sip:Feng.Liu@nyumc.org\", \"x400:C = us; A = ; P = NYUMC; O = Exchange; S = Liu2; G = Feng; \", \"X400:C = us; A = ; P = NYUMC; O = Exchange; S = Liu; G = Feng; \", \"smtp:liuf01@popmail.med.nyu.edu\", \"smtp:liuf01@med.nyu.edu\", \"smtp:liuf01@endeavor.med.nyu.edu\", \"smtp:feng.liu@med.nyu.edu\", \"smtp:liuf01@nyumc.org\", \"SMTP:Feng.Liu@nyumc.org\"], \"pwdLastSet\": \"130887216611363226\", \"sAMAccountName\": \"liuf01\", \"sAMAccountType\": \"805306368\", \"scriptPath\": \"logon.cmd\", \"showInAddressBook\": [\"CN = DefaultGlobalAddressList, CN = AllGlobalAddressLists, CN = AddressListsContainer, CN = NYUMC, CN = MicrosoftExchange, CN = Services, CN = Configuration, DC = nyumc, DC = org\", \"CN = AllUsers, CN = AllAddressLists, CN = AddressListsContainer, CN = NYUMC, CN = MicrosoftExchange, CN = Services, CN = Configuration, DC = nyumc, DC = org\"], \"sn\": \"Liu\", \"telephoneNumber\": \" + 12125627404\", \"textEncodedORAddress\": \"X400:C = us; A = ; P = NYUMC; O = Exchange; S = Liu; G = Feng; \", \"uSNChanged\": \"407033327\", \"uSNCreated\": \"293086\", \"uidNumber\": \"104563\", \"userAccountControl\": \"544\", \"userParameters\": \"PCtxCfgPresentCtxCfgFlags1CtxShadow*CtxMinEncryptionLevelCtxWFProfilePath\", \"userPrincipalName\": \"liuf01@NYUMC.ORG\", \"wWWHomePage\": \"June16\", \"whenChanged\": \"2016 - 03 - 08T17:03 : 54.00\", \"whenCreated\": \"2005 - 08 - 27T16 : 25 : 20.00\"}";
			//for (int i = 120; i <= 120; ++i)
			//{
			//	//auto j = Parse(s, i);
			//	//cout << i << " ----------------------------" << endl << j.dump() << endl;
			//	//string ev;
			//	string eu;
			//	//string p = j.dump();

			//	//for (int a = 0, b = 0; a < s.size() && b < p.size(); ++a, ++b)
			//	//{
			//	//	if (p[b] == '\\')
			//	//	{
			//	//		auto as = s.substr(a, s.size() - a);
			//	//		auto bs = p.substr(b, p.size() - b);

			//	//		++b;
			//	//	}
			//	//	
			//	//	if (s[a] != p[b])
			//	//	{
			//	//		auto as = s.substr(a, s.size() - a);
			//	//		auto bs = p.substr(b, p.size() - b);

			//	//		++a;
			//	//		++b;
			//	//	}
			//	//}

			//	Json::parse(s, eu);

			//	//vector<Json> u = { Json::parse(p, ev) };
			//	vector<Json> v = { Json::parse(s, eu) };
			//	//vector<Json> v = { j };
			//	ElasticClient::Index(v.begin(), v.end(), "192.168.230.131:9200", "a_index", "a_type");

			//  this_thread::sleep_for(chrono::milliseconds(1000));
			//	Delete();
			//}
		}

		void Execute()
		{
			Get();
			Search();
			Index();
			this_thread::sleep_for(chrono::milliseconds(1000));
			Get();
			Search();
			Index();
			this_thread::sleep_for(chrono::milliseconds(1000));
			Get();
			Search();
			Delete();
			this_thread::sleep_for(chrono::milliseconds(1000));
			Get();
			Search();
		}
	}

	void ExecutePeriodicUpsert()
	{
		// mongo::client::GlobalInstance must be instantiated in a function
		mongo::client::GlobalInstance mongoInstance;
		mongoInstance.assertInitialized();

		mutex m;
		unique_lock<mutex> lock(m);
		const chrono::seconds period(60);

		const string ldapUrl = "nyumc.org";
		const int ldapPort = 389;
		const string ldapLogin = "ostapm01@nyumc.org";
		const string ldapPassword = "q1W2e3R4t5";
		const string ldapNode = "OU=NYUMC Users,DC=nyumc,DC=org";
		const string ldapFilter = "";
		const string ldapIdAttribute = "cn";
		const string ldapCreateTimeAttribute = "whenCreated";
		const string ldapUpdateTimeAttribute = "whenChanged";
		time_t ldapLowerBound = -1;
		//time_t ldapLowerBound = LdapDateTime::ToTimeT("20160314100000.0Z");
		//const time_t ldapLowerBound = LdapDateTime::ToTimeT("20030808200000.0Z"); // -1
		//const time_t ldapUpperBound = LdapDateTime::ToTimeT("20030809000000.0Z"); // -1

		const string mongoUrl = "mongodb://192.168.230.131:27017";
		const string mongoDatabase = "nyu";
		const string mongoCollection = "mc_users";

		const string elasticUrl = "192.168.230.131:9200";
		const string elasticIndex = "nyu";
		const string elasticType = "mc_users";
		const string elasticChannel = "ldap:nyumc";

		const string lmdbPath = "C:/Outvoider/Projects/Nyulmc";
		const string lmdbKeyForLdapLowerBound = "ldapLowerBound";

		const string createdFileName = "C:/Outvoider/Projects/logs/ldap_last_created_log.txt";
		const string updatedFileName = "C:/Outvoider/Projects/logs/ldap_last_updated_log.txt";

		//spdlog::set_async_mode(1048576);
		spdlog::daily_logger_mt("logger", "C:/Outvoider/Projects/logs/ldap_log", 0, 0, true);

		//LmeClient::Upsert(ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapCreateTimeAttribute, ldapLowerBound, ldapUpperBound,
		//	mongoUrl, mongoDatabase, mongoCollection,
		//	elasticUrl, elasticIndex, elasticType, elasticChannel);

		//for (auto &o : MongoClient::Query(mongoUrl, mongoDatabase, mongoCollection))
		//{
		//	cout << o << endl;
		//}

		//cout << endl << endl << endl << endl << endl;
		//cout << ElasticClient::Search(elasticUrl, elasticIndex, elasticType).dump() << endl;

		//LmdbClient::Remove(lmdbPath, lmdbKeyForLdapLowerBound);
		//auto aaa = LmdbClient::Get(lmdbPath, lmdbKeyForLdapLowerBound);
		//LmdbClient::Set(lmdbPath, lmdbKeyForLdapLowerBound, "20160314100000.0Z");
		//aaa = LmdbClient::Get(lmdbPath, lmdbKeyForLdapLowerBound);
		//LmdbClient::Remove(lmdbPath, lmdbKeyForLdapLowerBound);
		//aaa = LmdbClient::Get(lmdbPath, lmdbKeyForLdapLowerBound);

		//spdlog::get("logger")->info() << "aaaaaaaaaaaaaaaaa";
		//spdlog::get("logger")->error() << "aaaaaaaaaaaaaaaaa";
		//spdlog::get("logger")->critical() << "aaaaaaaaaaaaaaaaa";
		//spdlog::get("logger")->flush();

		//MongoClient::DropDatabase(mongoUrl, mongoDatabase);
		//vector<string> ids;
		//ElasticClient::Delete(ids.begin(), ids.end(), elasticUrl, elasticIndex, elasticType);
		//LmdbClient::Remove(lmdbPath, lmdbKeyForLdapLowerBound);
		//LmdbClient::Set(lmdbPath, lmdbKeyForLdapLowerBound, "20160315100000.0Z");

		LmeClient::UpsertPeriodic(lock, period,
			ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapCreateTimeAttribute, ldapUpdateTimeAttribute,
			mongoUrl, mongoDatabase, mongoCollection,
			elasticUrl, elasticIndex, elasticType, elasticChannel,
			lmdbPath, lmdbKeyForLdapLowerBound,
			createdFileName, updatedFileName);
	}

	void Execute()
	{
		//ElasticClientTest::StringifyAttributes();
		//ElasticClientTest::PrintBinaryAttributes();
		//ElasticClientTest::Test();
		//ElasticClientTest::Execute();
		ExecutePeriodicUpsert();
	}

	//-------------------------------------------------------------------
	// TODO
	//
	//-------------------------------------------------------------------
}