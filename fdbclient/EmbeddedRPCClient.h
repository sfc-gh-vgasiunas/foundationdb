/*
 * EmbeddedRPCClient.h
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

#ifndef FDBCLIENT_EMBEDDEDRPCCLIENT_H
#define FDBCLIENT_EMBEDDEDRPCCLIENT_H
#pragma once

#include "fdbclient/ClientRPCInterface.h"
#include "flow/ThreadSafeQueue.h"

namespace ClientProxy {
struct ProxyState;
}

class EmbeddedRPCClient : public ClientRPCInterface {
public:
	static constexpr uint64_t CLIENT_ID = 1;
	EmbeddedRPCClient(std::string connFilename);
	~EmbeddedRPCClient() override;
	ThreadFuture<ClientProxy::ExecOperationsReply> executeOperations(
	    ClientProxy::ExecOperationsReference request) override;
	void releaseTransaction(uint64_t transaction) override;
	uint64_t getClientID() override { return CLIENT_ID; }

private:
	ClientProxy::ProxyState* proxyState;
	ThreadSafeQueue<uint64_t> releasedTransactions;
};

class DLRPCClient : public ClientRPCInterface {
public:
	static constexpr uint64_t CLIENT_ID = 1;
	DLRPCClient(std::string connFilename);
	~DLRPCClient() override;
	ThreadFuture<ClientProxy::ExecOperationsReply> executeOperations(
	    ClientProxy::ExecOperationsReference request) override;
	void releaseTransaction(uint64_t transaction) override;
	uint64_t getClientID() override { return CLIENT_ID; }

private:
	EmbeddedRPCClient* impl;
};

typedef struct FDB_future FDBFuture;
typedef int fdb_error_t;

extern "C" {
DLLEXPORT fdb_error_t fdb_rpc_client_create(const char* connFilename, EmbeddedRPCClient** client);
DLLEXPORT FDBFuture* fdb_rpc_client_exec_request(EmbeddedRPCClient* client, const void* requestBytes, int requestLen);
DLLEXPORT void fdb_rpc_client_release_transaction(EmbeddedRPCClient* client, uint64_t txID);
DLLEXPORT void fdb_rpc_client_destroy(EmbeddedRPCClient* client);
}

#endif