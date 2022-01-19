/*
 * EmbeddedRPCClient.actor.cpp
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

#include "flow/actorcompiler.h" // This must be the last #include.

using namespace ClientProxy;

namespace {

ACTOR
Future<Void> executeRemoteProxyOperationsActor(Future<Void> signal,
                                               Reference<ClientProxyRPCStub> client,
                                               ExecOperationsReference request,
                                               ThreadSingleAssignmentVar<ExecOperationsReply>* result) {
	try {
		wait(signal);
		while (true) {
			Optional<uint64_t> t = client->releasedTransactions.pop();
			if (!t.present())
				break;
			request->releasedTransactions.push_back(t.get());
		}
		ExecOperationsReply r = wait(client->interface.execOperations.getReply(*request));
		result->send(r);
	} catch (Error& e) {
		result->sendError(e);
	}

	ThreadFuture<ExecOperationsReply> destroyResultAfterReturning(
	    result); // Call result->delref(), but only after our return promise is no longer referenced on this thread
	return Void();
}

} // namespace

ClientProxyRPCStub::ClientProxyRPCStub(std::string proxyUrl) {
	std::cout << "Connecting to proxy " << proxyUrl << std::endl;
	NetworkAddress proxyAddress = NetworkAddress::parse(proxyUrl);
	interface.initClientEndpoints(proxyAddress);
	clientID = deterministicRandom()->randomUInt64();
}

void ClientProxyRPCStub::executeOperations(ExecOperationsReference request,
                                           ThreadSingleAssignmentVar<ExecOperationsReply>* result) {
	Reference<ClientProxyRPCStub> thisRef = Reference<ClientProxyRPCStub>::addRef(this);
	Promise<Void> signal;
	Future<Void> cancelFuture = executeRemoteProxyOperationsActor(signal.getFuture(), thisRef, request, result);
	result->setCancel(std::move(cancelFuture));
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
}

void ClientProxyRPCStub::releaseTransaction(uint64_t transactionID) {
	releasedTransactions.push(transactionID);
}

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

namespace {

ACTOR
Future<Void> executeEmbeddedProxyOperationsActor(Future<Void> signal,
                                                 Reference<EmbeddedRPCClient> client,
                                                 ExecOperationsReference request,
                                                 ThreadSingleAssignmentVar<ExecOperationsReply>* result) {
	try {
		wait(signal);
		while (true) {
			Optional<uint64_t> t = client->releasedTransactions.pop();
			if (!t.present())
				break;
			ClientProxy::releaseTransaction(client->proxyState, UID(EmbeddedRPCClient::CLIENT_ID, t.get()));
		}
		handleExecOperationsRequest(client->proxyState, request);
		ExecOperationsReply r = wait(request->reply.getFuture());
		result->send(r);
	} catch (Error& e) {
		result->sendError(e);
	}

	ThreadFuture<ExecOperationsReply> destroyResultAfterReturning(
	    result); // Call result->delref(), but only after our return promise is no longer referenced on this thread
	return Void();
}

} // namespace

void EmbeddedRPCClient::executeOperations(ExecOperationsReference request,
                                          ThreadSingleAssignmentVar<ClientProxy::ExecOperationsReply>* result) {
	Reference<EmbeddedRPCClient> thisRef = Reference<EmbeddedRPCClient>::addRef(this);
	Promise<Void> signal;
	Future<Void> cancelFuture = executeEmbeddedProxyOperationsActor(signal.getFuture(), thisRef, request, result);
	result->setCancel(std::move(cancelFuture));
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
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

void DLRPCClient::executeOperations(ExecOperationsReference request,
                                    ThreadSingleAssignmentVar<ClientProxy::ExecOperationsReply>* result) {
	ObjectWriter ow(Unversioned());
	ow.serialize(*(ExecOperationsReqInput*)request.getPtr());
	StringRef str = ow.toStringRef();
	fdb_rpc_client_exec_request(impl, str.begin(), str.size(), (FDBFuture*)result);
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

extern "C" DLLEXPORT void fdb_rpc_client_exec_request(EmbeddedRPCClient* client,
                                                      const void* requestBytes,
                                                      int requestLen,
                                                      FDBFuture* result) {
	try {
		ObjectReader rd((const uint8_t*)requestBytes, Unversioned());
		ExecOperationsReference ref = makeReference<ExecOperationsRequestRefCounted>();
		rd.deserialize(*(ExecOperationsReqInput*)ref.getPtr());
		client->executeOperations(ref, (ThreadSingleAssignmentVar<ClientProxy::ExecOperationsReply>*)result);
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
