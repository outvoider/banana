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

		string ToUtc(const string &dt, const bool hasT = true, const bool hasZ = true)
		{
			stringstream s;

			s << dt.substr(0, 4) << "-"
				<< dt.substr(4, 2) << "-"
				<< dt.substr(6, 2) << (hasT ? "T" : " ")
				<< dt.substr(8, 2) << ":"
				<< dt.substr(10, 2) << ":"
				<< dt.substr(12, 2) << ".00";

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

				auto utcLower = i.first >= 0 ? LdapDateTime::ToUtc(lower, false, false) : "";
				auto utcUpper = i.second >= 0 ? LdapDateTime::ToUtc(upper, false, false) : "";
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
				spdlog::get("logger")->critical() << "mongo create has failed";
				return nullptr;
			}

			mongo::DBClientBase *client = cs.connect(errmsg);
			client ? spdlog::get("logger")->info() << "mongo create has finished" : spdlog::get("logger")->critical() << "mongo create has failed";
			return client;
		}

		void Upsert(vector<mongo::BSONObj>::const_iterator begin
			, vector<mongo::BSONObj>::const_iterator end
			, const string &url
			, const string &database
			, const string &table)
		{
			if (begin != end)
			{
				boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
				auto builder = client->initializeUnorderedBulkOp(database + "." + table);

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
			, const string &table)
		{
			vector<mongo::BSONObj> objects;
			boost::scoped_ptr<mongo::DBClientBase> client(Create(url));
			auto cursor = client->query(database + "." + table, mongo::BSONObj());

			if (!cursor.get())
			{
				spdlog::get("logger")->error() << "mongo query has failed";
				return objects;
			}

			while (cursor->more())
			{
				objects.push_back(cursor->next().copy());
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
			string error;
			stringstream s;
			HttpClient client(url);

			for (int i = from, n = min(count, batchCount); n != 0; i += n)
			{
				s.str(""); if (query != "") s << query << endl;
				auto path = (index == "" ? "" : "/" + index + (type == "" ? "" : "/" + type)) + "/_search?from=" + to_string(i) + "&size=" + to_string(n);
				auto response = client.request("GET", path, s);
				s.str(""); s << response->content.rdbuf();
				spdlog::get("logger")->info() << s.str();
				auto json = Json::parse(s.str(), error);

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
			string value;
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

			spdlog::get("logger")->info() << (rc != 0 ? "lmdb get has failed" : "lmdb get has finished");

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

			spdlog::get("logger")->info() << (rc != 0 ? "lmdb set has failed" : "lmdb set has finished");
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

			Json operator()(const LDAPEntry *entry, const string &eId)
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
								jValues.push_back(LdapDateTime::ToUtc(v, false));
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
						an == eId ? "_id" : an,
						(0 == vl.size()) ? nullptr : (1 == vl.size()) ? jValues[0] : jValues
					});
				}

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

			mongo::BSONObj operator()(const LDAPEntry *entry, const string &eId)
			{
				mongo::BSONObjBuilder oBuilder;
				auto al = entry->getAttributes();

				for (auto a = al->begin(); a != al->end(); ++a)
				{
					mongo::BSONArrayBuilder aBuilder;
					auto an = a->getName();
					auto vl = a->getValues();
					auto id = an == eId ? "_id" : an;

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
			, const string &mongoTable

			, const string &elasticUrl
			, const string &elasticIndex
			, const string &elasticType

			, const string &fileName)
		{
			LdapEntry::ToJson toJson;
			vector<Json> jEntries;
			LdapEntry::ToBson toBson;
			vector<mongo::BSONObj> bEntries;
			ofstream backup(fileName);

			LdapClient::Search(ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapTimeAttribute, ldapLowerBound, ldapUpperBound, maxTime, [&](const LDAPEntry *entry)
			{
				jEntries.push_back(toJson(entry, ldapIdAttribute));
				bEntries.push_back(toBson(entry, ldapIdAttribute));

				backup << jEntries.back().dump() << endl;
				backup.flush();
			});

			backup.close();
			ElasticClient::Index(jEntries.begin(), jEntries.end(), elasticUrl, elasticIndex, elasticType);
			MongoClient::Upsert(bEntries.begin(), bEntries.end(), mongoUrl, mongoDatabase, mongoTable);

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
			, const string &mongoTable

			, const string &elasticUrl
			, const string &elasticIndex
			, const string &elasticType

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
					mongoUrl, mongoDatabase, mongoTable,
					elasticUrl, elasticIndex, elasticType,
					createdFileName);

				LmeClient::Upsert(ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapUpdateTimeAttribute, ldapLowerBound, -1, maxTime,
					mongoUrl, mongoDatabase, mongoTable,
					elasticUrl, elasticIndex, elasticType,
					updatedFileName);

				ldapLowerBound = maxTime - (system_clock::to_time_t(system_clock::now()) - system_clock::to_time_t(end));
				LmdbClient::Set(lmdbPath, lmdbKeyForLdapLowerBound, LdapDateTime::FromTimeT(ldapLowerBound));

				spdlog::get("logger")->info() << "lme upsertperiodic has finished, next in " << period.count() << " seconds";
			}
			while (cv.wait_for(lock, period) == cv_status::timeout);
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

		const string ldapUrl = "";
		const int ldapPort = ;
		const string ldapLogin = "";
		const string ldapPassword = "";
		const string ldapNode = "";
		const string ldapFilter = "";
		const string ldapIdAttribute = "";
		const string ldapCreateTimeAttribute = "";
		const string ldapUpdateTimeAttribute = "";
		time_t ldapLowerBound = -1;
		//time_t ldapLowerBound = LdapDateTime::ToTimeT("20160314100000.0Z");
		//const time_t ldapLowerBound = LdapDateTime::ToTimeT("20030808200000.0Z"); // -1
		//const time_t ldapUpperBound = LdapDateTime::ToTimeT("20030809000000.0Z"); // -1

		const string mongoUrl = "";
		const string mongoDatabase = "";
		const string mongoTable = "";

		const string elasticUrl = "";
		const string elasticIndex = "";
		const string elasticType = "";

		const string lmdbPath = "";
		const string lmdbKeyForLdapLowerBound = "";

		const string createdFileName = "logs/ldap_last_created_log.txt";
		const string updatedFileName = "logs/ldap_last_updated_log.txt";

		//spdlog::set_async_mode(1048576);
		spdlog::daily_logger_mt("logger", "logs/ldap_log", 0, 0, true);

		//LmeClient::Upsert(ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapCreateTimeAttribute, ldapLowerBound, ldapUpperBound,
		//	mongoUrl, mongoDatabase, mongoTable,
		//	elasticUrl, elasticIndex, elasticType);

		//for (auto &o : MongoClient::Query(mongoUrl, mongoDatabase, mongoTable))
		//{
		//	cout << o << endl;
		//}

		//cout << endl << endl << endl << endl << endl;
		//cout << ElasticClient::Search(elasticUrl, elasticIndex, elasticType).dump() << endl;

		//MongoClient::DropDatabase(mongoUrl, mongoDatabase);
		//vector<string> ids;
		//ElasticClient::Delete(ids.begin(), ids.end(), elasticUrl, elasticIndex, elasticType);
		//LmdbClient::Remove(lmdbPath, lmdbKeyForLdapLowerBound);
		//LmdbClient::Set(lmdbPath, lmdbKeyForLdapLowerBound, "20160315100000.0Z");

		LmeClient::UpsertPeriodic(lock, period,
			ldapUrl, ldapPort, ldapLogin, ldapPassword, ldapNode, ldapFilter, ldapIdAttribute, ldapCreateTimeAttribute, ldapUpdateTimeAttribute,
			mongoUrl, mongoDatabase, mongoTable,
			elasticUrl, elasticIndex, elasticType,
			lmdbPath, lmdbKeyForLdapLowerBound,
			createdFileName, updatedFileName);
	}

	void Execute()
	{
		ExecutePeriodicUpsert();
	}
}