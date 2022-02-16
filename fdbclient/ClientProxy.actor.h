/*
 * ClientProxy.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_CLIENTPROXY_ACTOR_G_H)
#define FDBCLIENT_CLIENTPROXY_ACTOR_G_H
#include "fdbclient/ClientProxy.actor.g.h"
#elif !defined(FDBCLIENT_CLIENTPROXY_ACTOR_H)
#define FDBCLIENT_CLIENTPROXY_ACTOR_H

#include "flow/flow.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ClientProxyInterface.h"
#include "fdbclient/ClientRPCInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/actorcompiler.h"

namespace ClientProxy {

struct ProxyRequestState;
using ProxyRequestReference = Reference<ProxyRequestState>;

struct ProxyTransaction : public ReferenceCounted<ProxyTransaction> {
	Reference<ReadYourWritesTransaction> tx;
	uint32_t lastExecSeqNo = 0;
	std::unordered_map<uint32_t, ProxyRequestReference> pendingRequests;
	std::unordered_map<uint32_t, ProxyRequestReference> activeRequests;
};

using ProxyTransactionReference = Reference<ProxyTransaction>;

struct ProxyState {
	Database db;
	using TransactionMap = std::unordered_map<UID, ProxyTransactionReference>;
	TransactionMap transactionMap;

	ProxyState(Reference<IClusterConnectionRecord> connRecord, LocalityData clientLocality);

	ProxyTransactionReference getTransaction(UID transactionID);

	void releaseTransaction(UID transactionID);
};

struct ProxyRequestState : public ReferenceCounted<ProxyRequestState> {
	enum class State { PENDING, STARTED, COMPLETED };

	State st = State::PENDING;
	ExecOperationsReference request;
	ProxyTransactionReference proxyTransaction;
	std::vector<Future<Void>> execActors;
	IExecOperationsCallbackHandler* callbackHandler = nullptr;
	IExecOperationsCallback* execCallback = nullptr;

	Reference<ReadYourWritesTransaction> transaction() { return proxyTransaction->tx; }
	void actorStarted(Future<Void> actor);
	void sendReply(const ExecOperationsReply& reply);
	void sendError(const Error& e);
	void completed();
	void cancel();
	bool isCompleted() { return st == State::COMPLETED; }
};

ProxyState* createProxyState(Reference<IClusterConnectionRecord> connRecord, LocalityData clientLocality);

void destroyProxyState(ProxyState* proxyState);

ProxyRequestReference handleExecOperationsRequest(ProxyState* rpcProxyData,
                                                  ExecOperationsReference request,
                                                  IExecOperationsCallbackHandler* callbackHandler,
                                                  IExecOperationsCallback* cb);

void releaseTransaction(ProxyState* proxyState, UID transactionID);

ACTOR Future<Void> proxyServer(ClientProxyInterface interface,
                               Reference<IClusterConnectionRecord> connRecord,
                               LocalityData clientLocality);

} // namespace ClientProxy

#endif