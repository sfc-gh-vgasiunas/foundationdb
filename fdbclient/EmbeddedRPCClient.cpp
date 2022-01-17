/*
 * EmbeddedRPCClient.cpp
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

#include "fdbclient/EmbeddedRPCClient.h"
#include "fdbclient/ClientProxy.actor.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "flow/ThreadHelper.actor.h"

EmbeddedRPCClient::EmbeddedRPCClient(std::string connFilename) {
	Reference<IClusterConnectionRecord> connFile =
	    makeReference<ClusterConnectionFile>(ClusterConnectionFile::lookupClusterFileName(connFilename).first);
	Reference<EmbeddedRPCClient> thisRef = Reference<EmbeddedRPCClient>::addRef(this);
	onMainThreadVoid(
	    [thisRef, connFile]() { thisRef->proxyState = ClientProxy::createProxyState(connFile, LocalityData()); });
}

EmbeddedRPCClient::~EmbeddedRPCClient() {
	ClientProxy::ProxyState* proxyState = this->proxyState;
	onMainThreadVoid([proxyState]() { ClientProxy::destroyProxyState(proxyState); });
}

ThreadFuture<ClientProxy::ExecOperationsReply> EmbeddedRPCClient::executeOperations(ExecOperationsReference request) {
	Reference<EmbeddedRPCClient> thisRef = Reference<EmbeddedRPCClient>::addRef(this);
	return onMainThread([thisRef, request]() {
		while (true) {
			Optional<uint64_t> t = thisRef->releasedTransactions.pop();
			if (!t.present())
				break;
			ClientProxy::releaseTransaction(thisRef->proxyState, UID(CLIENT_ID, t.get()));
		}
		ClientProxy::handleExecOperationsRequest(thisRef->proxyState, *request);
		return request->reply.getFuture();
	});
}

void EmbeddedRPCClient::releaseTransaction(uint64_t transaction) {
	releasedTransactions.push(transaction);
}
