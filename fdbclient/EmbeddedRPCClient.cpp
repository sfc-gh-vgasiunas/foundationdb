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

using namespace ClientProxy;

EmbeddedRPCClient::EmbeddedRPCClient(std::string connFilename) {
	Reference<IClusterConnectionRecord> connFile =
	    makeReference<ClusterConnectionFile>(ClusterConnectionFile::lookupClusterFileName(connFilename).first);
	Reference<EmbeddedRPCClient> thisRef = Reference<EmbeddedRPCClient>::addRef(this);
	onMainThreadVoid([thisRef, connFile]() { thisRef->proxyState = createProxyState(connFile, LocalityData()); });
}

EmbeddedRPCClient::~EmbeddedRPCClient() {
	ProxyState* proxyState = this->proxyState;
	onMainThreadVoid([proxyState]() { destroyProxyState(proxyState); });
}

ThreadFuture<ExecOperationsReply> EmbeddedRPCClient::executeOperations(ExecOperationsReference request) {
	Reference<EmbeddedRPCClient> thisRef = Reference<EmbeddedRPCClient>::addRef(this);
	return onMainThread([thisRef, request]() {
		while (true) {
			Optional<uint64_t> t = thisRef->releasedTransactions.pop();
			if (!t.present())
				break;
			ClientProxy::releaseTransaction(thisRef->proxyState, UID(CLIENT_ID, t.get()));
		}
		handleExecOperationsRequest(thisRef->proxyState, request);
		return request->reply.getFuture();
	});
}

void EmbeddedRPCClient::releaseTransaction(uint64_t transaction) {
	releasedTransactions.push(transaction);
}

DLRPCClient::DLRPCClient(std::string connFilename) {
	fdb_error_t err = fdb_rpc_client_create(connFilename.c_str(), &impl);
	if (err != error_code_success) {
		throw Error(err);
	}
}

DLRPCClient::~DLRPCClient() {
	fdb_rpc_client_destroy(impl);
}

ThreadFuture<ExecOperationsReply> DLRPCClient::executeOperations(ExecOperationsReference request) {
	ObjectWriter ow(Unversioned());
	ow.serialize(*(ExecOperationsReqInput*)request.getPtr());
	StringRef str = ow.toStringRef();
	ThreadSingleAssignmentVar<ExecOperationsReply>* sav =
	    (ThreadSingleAssignmentVar<ExecOperationsReply>*)fdb_rpc_client_exec_request(impl, str.begin(), str.size());
	return ThreadFuture<ExecOperationsReply>(sav);
}

void DLRPCClient::releaseTransaction(uint64_t transaction) {
	fdb_rpc_client_release_transaction(impl, transaction);
}

extern "C" DLLEXPORT fdb_error_t fdb_rpc_client_create(const char* connFilename, EmbeddedRPCClient** client) {
	try {
		*client = new EmbeddedRPCClient(connFilename);
	} catch (Error& e) {
		return e.code();
	}
	return error_code_success;
}

extern "C" DLLEXPORT FDBFuture* fdb_rpc_client_exec_request(EmbeddedRPCClient* client,
                                                            const void* requestBytes,
                                                            int requestLen) {
	try {
		ObjectReader rd((const uint8_t*)requestBytes, Unversioned());
		ExecOperationsReference ref = makeReference<ExecOperationsRequestRefCounted>();
		rd.deserialize(*(ExecOperationsReqInput*)ref.getPtr());
		ThreadSingleAssignmentVarBase* sav = client->executeOperations(ref).extractPtr();
		return (FDBFuture*)sav;
	} catch (Error& e) {
		fprintf(stderr, "fdb_rpc_client_exec_request: Unexpected FDB error %d\n", e.code());
		abort();
	} catch (...) {
		fprintf(stderr, "fdb_rpc_client_exec_request: Unexpected FDB unknown error\n");
		abort();
	}
}

extern "C" DLLEXPORT void fdb_rpc_client_release_transaction(EmbeddedRPCClient* client, uint64_t txID) {
	client->releaseTransaction(txID);
}

extern "C" DLLEXPORT void fdb_rpc_client_destroy(EmbeddedRPCClient* client) {
	delete client;
}
