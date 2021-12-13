/*
 * client_proxy_tests.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define FDB_API_VERSION 710
#include <foundationdb/fdb_c.h>

#include <iostream>
#include <string.h>
#include <thread>
#include <map>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest.h"
#include "fdb_api.hpp"

static std::string KEY_PREFIX = "proxytest/";

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}

FDBDatabase* fdb_open_database() {
	FDBDatabase* db;
	fdb_check(fdb_create_database("", &db));
	return db;
}

static FDBDatabase* db = nullptr;

// Blocks until the given future is ready, returning an error code if there was
// an issue.
fdb_error_t wait_future(fdb::Future& f) {
	fdb_check(f.block_until_ready());
	return f.get_error();
}

bool needRetry(fdb::Transaction& tr, fdb_error_t err) {
	if (err) {
		std::cout << "Retrying on error: " << fdb_get_error(err) << std::endl;
		fdb::EmptyFuture f2 = tr.on_error(err);
		fdb_check(wait_future(f2));
		return true;
	}
	return false;
}

std::string dbKey(const std::string& key) {
	return KEY_PREFIX + key;
}

// Clears all data in the database, then inserts the given key value pairs.
void insert_data(FDBDatabase* db, const std::map<std::string, std::string>& data) {
	fdb::Transaction tr(db);
	while (1) {
		for (const auto& [key, val] : data) {
			tr.set(key, val);
		}

		fdb::EmptyFuture f1 = tr.commit();
		fdb_error_t err = wait_future(f1);
		if (needRetry(tr, err)) {
			continue;
		}
		std::cout << "Commit successful" << std::endl;
		break;
	}
}

// Helper function to add `prefix` to all keys in the given map. Returns a new
// map.
std::map<std::string, std::string> create_data(std::map<std::string, std::string>&& map) {
	std::map<std::string, std::string> out;
	for (const auto& [key, val] : map) {
		out[dbKey(key)] = val;
	}
	return out;
}

std::string getFutureValue(fdb::ValueFuture& f) {
	int out_present;
	char* val;
	int vallen;
	fdb_check(f.get(&out_present, (const uint8_t**)&val, &vallen));
	CHECK(out_present);
	return std::string(val, vallen);
}

void checkFutureValue(fdb::ValueFuture& f, const char* expectedVal) {
	std::string val = getFutureValue(f);
	CHECK(val.compare(expectedVal) == 0);
}

void checkKeyValue(FDBKeyValue kv, const char* expectedKey, const char* expectedVal) {
	std::string key((const char*)kv.key, kv.key_length);
	std::string value((const char*)kv.value, kv.value_length);

	CHECK(key.compare(dbKey(expectedKey)) == 0);
	CHECK(value.compare(expectedVal) == 0);
}

void checkKeyValueInDB(const char* key, const char* expectedVal) {
	fdb::Transaction tr(db);
	while (1) {
		fdb::ValueFuture f1 = tr.get(dbKey(key), /* snapshot */ false);
		fdb_error_t err = wait_future(f1);
		if (needRetry(tr, err)) {
			continue;
		}
		checkFutureValue(f1, expectedVal);
		break;
	}
}

TEST_CASE("fdb_get_value") {
	insert_data(db, create_data({ { "foo", "1" }, { "baz", "2" }, { "bar", "3" } }));

	fdb::Transaction tr(db);
	while (1) {
		fdb::ValueFuture f1 = tr.get(dbKey("foo"), /* snapshot */ false);
		fdb::ValueFuture f2 = tr.get(dbKey("baz"), /* snapshot */ false);
		fdb::ValueFuture f3 = tr.get(dbKey("bar"), /* snapshot */ false);
		fdb_error_t err1 = wait_future(f1);
		fdb_error_t err2 = wait_future(f2);
		fdb_error_t err3 = wait_future(f3);
		fdb_error_t err = err1 ? err1 : (err2 ? err2 : err3);
		if (needRetry(tr, err)) {
			continue;
		}
		checkFutureValue(f1, "1");
		checkFutureValue(f2, "2");
		checkFutureValue(f3, "3");
		break;
	}
}

TEST_CASE("fdb_read_update") {
	insert_data(db, create_data({ { "foo", "1" } }));

	fdb::Transaction tr(db);
	while (1) {
		fdb::ValueFuture f1 = tr.get(dbKey("foo"), /* snapshot */ false);
		fdb_error_t err1 = wait_future(f1);
		if (needRetry(tr, err1)) {
			continue;
		}

		std::string val = getFutureValue(f1);
		val[0] = val[0] + 1;

		tr.set(dbKey("foo"), val);
		fdb::EmptyFuture f2 = tr.commit();
		fdb_error_t err2 = wait_future(f2);
		if (needRetry(tr, err2)) {
			continue;
		}
		break;
	}

	checkKeyValueInDB("foo", "2");
}

TEST_CASE("fdb_get_range") {
	insert_data(db, create_data({ { "range1", "1" }, { "range2", "2" }, { "outofrange", "3" } }));

	fdb::Transaction tr(db);
	while (1) {
		std::string begin = dbKey("range1");
		std::string end = dbKey("range9");

		fdb::KeyValueArrayFuture f1 = tr.get_range((uint8_t*)begin.c_str(),
		                                           begin.size(),
		                                           true, // begin or equal
		                                           0, // begin offset
		                                           (uint8_t*)end.c_str(),
		                                           end.size(),
		                                           true, // end or equal
		                                           100, // end offset
		                                           100, // limit rows
		                                           0, // limit bytes
		                                           FDB_STREAMING_MODE_WANT_ALL,
		                                           0, // iteration
		                                           false, // snapshot
		                                           false); // reverse

		fdb_error_t err = wait_future(f1);
		if (needRetry(tr, err)) {
			continue;
		}

		FDBKeyValue const* out_kv;
		int out_count;
		int out_more;
		fdb_check(f1.get(&out_kv, &out_count, &out_more));

		CHECK(out_count == 2);
		CHECK(out_more == 0);
		checkKeyValue(out_kv[0], "range1", "1");
		checkKeyValue(out_kv[1], "range2", "2");
		break;
	}
}

int main(int argc, char** argv) {
	if (argc < 2) {
		std::cout << "Client proxy unit tests for the FoundationDB C API.\n"
		          << "Usage: fdb_c_proxy_tests <proxy_url> [doctest args]" << std::endl;
		return 1;
	}

	fdb_check(fdb_select_api_version(710));
	std::string proxyUrl = argv[1];
	fdb_check(fdb_network_set_option(FDBNetworkOption::FDB_NET_OPTION_PROXY_URL,
	                                 reinterpret_cast<const uint8_t*>(proxyUrl.c_str()),
	                                 proxyUrl.size()));

	doctest::Context context;
	context.applyCommandLine(argc, argv);

	fdb_check(fdb_setup_network());
	std::thread network_thread{ &fdb_run_network };

	db = fdb_open_database();

	int res = context.run();

	std::cout << "Tearing down\n";

	fdb_database_destroy(db);

	fdb_check(fdb_stop_network());
	network_thread.join();

	std::cout << "Tear down completed\n";
	return res;
}
