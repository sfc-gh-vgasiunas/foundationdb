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
#include "fdbclient/FDBProxyCApi.h"

namespace ClientProxy {
struct ProxyState;
} // namespace ClientProxy

class EmbeddedRPCClient : public IClientRPCInterface, public IExecOperationsCallbackHandler {
public:
	static constexpr uint64_t CLIENT_ID = 1;
	EmbeddedRPCClient(const std::string& connFilename);
	~EmbeddedRPCClient() override;
	void executeOperations(ClientProxy::ExecOperationsReference request, IExecOperationsCallback* callback) override;
	void releaseTransaction(uint64_t transaction) override;
	uint64_t getClientID() override { return CLIENT_ID; }

	void sendReply(const ClientProxy::ExecOperationsReply& reply, IExecOperationsCallback* callback) override;

	void sendError(const Error& e, IExecOperationsCallback* callback) override;

	void cancelRequest(ClientProxy::ProxyRequestState* request) override;

	virtual void setProxyRequest(IExecOperationsCallback* callback, ClientProxy::ProxyRequestState* proxyReq);

	ClientProxy::ProxyState* proxyState;
	ThreadSafeQueue<uint64_t> releasedTransactions;
};

class ExternalRPCClient : public EmbeddedRPCClient {
public:
	ExternalRPCClient(const std::string& connFilename, FDBProxyRequestCallbackIfc callbackIfc)
	  : EmbeddedRPCClient(connFilename), callbackIfc(callbackIfc) {}

	void sendReply(const ClientProxy::ExecOperationsReply& reply, IExecOperationsCallback* callback) override;

	void sendError(const Error& e, IExecOperationsCallback* callback) override;

	void setProxyRequest(IExecOperationsCallback* callback, ClientProxy::ProxyRequestState* proxyReq) override;

private:
	FDBProxyRequestCallbackIfc callbackIfc;
};

class DLRPCClient : public IClientRPCInterface {
public:
	static constexpr uint64_t CLIENT_ID = 1;
	DLRPCClient(std::string connFilename, Reference<FDBProxyCApi> proxyApi);
	~DLRPCClient() override;
	void executeOperations(ClientProxy::ExecOperationsReference request, IExecOperationsCallback* callback) override;
	void releaseTransaction(uint64_t transaction) override;
	uint64_t getClientID() override { return CLIENT_ID; }
	void cancelRequest(ClientProxy::ProxyRequestState* request) override;

private:
	FDBProxy* impl;
	Reference<FDBProxyCApi> proxyApi;
};

class ClientProxyRPCStub : public IClientRPCInterface {
public:
	ClientProxyRPCStub(std::string proxyUrl);
	void executeOperations(ClientProxy::ExecOperationsReference request, IExecOperationsCallback* callback) override;
	void releaseTransaction(uint64_t transactionID) override;
	uint64_t getClientID() override { return clientID; }
	void cancelRequest(ClientProxy::ProxyRequestState* request) override { /* do nothing */
	}
	ThreadSafeQueue<uint64_t> releasedTransactions;
	ClientProxyInterface interface;
	uint64_t clientID;
};

#endif