/*
 * FDBProxyCApi.h
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

#ifndef FDBCLIENT_FDBPROXYCAPI_H
#define FDBCLIENT_FDBPROXYCAPI_H
#pragma once

#include "bindings/c/foundationdb/fdb_c_options.g.h"
#include "flow/FastRef.h"

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

typedef struct FDB_ProxyRequestCallback FDBProxyRequestCallback;
typedef struct FDB_ProxyRequest FDBProxyRequest;
typedef struct FDB_Proxy FDBProxy;

typedef int fdb_error_t;

struct FDBProxyRequestCallbackIfc {
	void (*sendReply)(FDBProxyRequestCallback* callback, const void* replyBytes, int replyLen);
	void (*sendError)(FDBProxyRequestCallback* callback, fdb_error_t error);
	void (*setProxyRequest)(FDBProxyRequestCallback* callback, FDBProxyRequest* proxyRequest);
};

extern "C" {
DLLEXPORT fdb_error_t fdb_proxy_create(const char* connFilename,
                                       FDBProxyRequestCallbackIfc callbackIfc,
                                       FDBProxy** proxy);
DLLEXPORT void fdb_proxy_exec_request(FDBProxy* proxy,
                                      const void* requestBytes,
                                      int requestLen,
                                      FDBProxyRequestCallback* result);
DLLEXPORT void fdb_proxy_release_transaction(FDBProxy* proxy, uint64_t txID);
DLLEXPORT void fdb_proxy_destroy(FDBProxy* proxy);
DLLEXPORT void fdb_proxy_cancel_request(FDBProxy* proxy, FDBProxyRequest* proxyRequest);
}

struct FDBProxyCApi : public ThreadSafeReferenceCounted<FDBProxyCApi> {
	// Proxy
	fdb_error_t (*createProxy)(const char* connFilename, FDBProxyRequestCallbackIfc callbackIfc, FDBProxy** proxy);
	void (*destroyProxy)(FDBProxy* proxy);
	void (*execRequest)(FDBProxy* proxy, const void* requestBytes, int requestLen, FDBProxyRequestCallback* callback);
	void (*releaseTransaction)(FDBProxy* proxy, uint64_t txID);
	void (*cancelRequest)(FDBProxy* proxy, FDBProxyRequest* proxyRequest);
};

#endif