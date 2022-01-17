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
	proxyServerActor = onMainThread(
	    [thisRef, connFile]() { return ClientProxy::proxyServer(thisRef->proxyInterface, connFile, LocalityData()); });
}

ThreadFuture<ClientProxy::ExecOperationsReply> EmbeddedRPCClient::executeOperations(ExecOperationsReference request) {
	return onMainThread([this, request]() {
		while (true) {
			Optional<uint64_t> t = releasedTransactions.pop();
			if (!t.present())
				break;
			request->releasedTransactions.push_back(t.get());
		}
		return proxyInterface.execOperations.getReply(*request);
	});
}

void EmbeddedRPCClient::releaseTransaction(uint64_t transaction) {
	releasedTransactions.push(transaction);
}
