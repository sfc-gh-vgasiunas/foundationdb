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
                                               ExecOperationsReplySAV* result) {
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
	result->delref();
	return Void();
}

} // namespace

ClientProxyRPCStub::ClientProxyRPCStub(std::string proxyUrl) {
	std::cout << "Connecting to proxy " << proxyUrl << std::endl;
	NetworkAddress proxyAddress = NetworkAddress::parse(proxyUrl);
	interface.initClientEndpoints(proxyAddress);
	clientID = deterministicRandom()->randomUInt64();
}

void ClientProxyRPCStub::executeOperations(ExecOperationsReference request, ExecOperationsReplySAV* result) {
	Reference<ClientProxyRPCStub> thisRef = Reference<ClientProxyRPCStub>::addRef(this);
	Promise<Void> signal;
	Future<Void> cancelFuture = executeRemoteProxyOperationsActor(signal.getFuture(), thisRef, request, result);
	result->setCancel(std::move(cancelFuture));
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
}

void ClientProxyRPCStub::releaseTransaction(uint64_t transactionID) {
	releasedTransactions.push(transactionID);
}

EmbeddedRPCClient::EmbeddedRPCClient(const std::string& connFilename) {
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
                                                 ExecOperationsReplySAV* result) {
	try {
		wait(signal);
		while (true) {
			Optional<uint64_t> t = client->releasedTransactions.pop();
			if (!t.present())
				break;
			releaseTransaction(client->proxyState, UID(EmbeddedRPCClient::CLIENT_ID, t.get()));
		}
		handleExecOperationsRequest(client->proxyState, request);
		ExecOperationsReply r = wait(request->reply.getFuture());
		client->sendReply(r, result);
	} catch (Error& e) {
		client->sendError(e, result);
	}
	return Void();
}

} // namespace

void EmbeddedRPCClient::executeOperations(ExecOperationsReference request, ExecOperationsReplySAV* result) {
	Reference<EmbeddedRPCClient> thisRef = Reference<EmbeddedRPCClient>::addRef(this);
	Promise<Void> signal;
	Future<Void> cancelFuture = executeEmbeddedProxyOperationsActor(signal.getFuture(), thisRef, request, result);
	result->setCancel(std::move(cancelFuture));
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
}

void EmbeddedRPCClient::releaseTransaction(uint64_t transaction) {
	releasedTransactions.push(transaction);
}

void EmbeddedRPCClient::sendReply(const ExecOperationsReply& reply, ExecOperationsReplySAV* result) {
	result->send(reply);
	result->delref();
}

void EmbeddedRPCClient::sendError(const Error& e, ExecOperationsReplySAV* result) {
	result->sendError(e);
	result->delref();
}

void ExternalRPCClient::sendReply(const ExecOperationsReply& reply, ExecOperationsReplySAV* result) {
	ObjectWriter ow(Unversioned());
	ow.serialize(reply);
	StringRef str = ow.toStringRef();
	sendReplyCB((FDBFuture*)result, str.begin(), str.size());
}

void ExternalRPCClient::sendError(const Error& e, ExecOperationsReplySAV* result) {
	sendErrorCB((FDBFuture*)result, e.code());
}

void DLRPCClient_sendReply(FDBFuture* f, const void* bytes, int size) {
	try {
		ObjectReader rd((const uint8_t*)bytes, Unversioned());
		ExecOperationsReply reply;
		rd.deserialize(reply);
		ExecOperationsReplySAV* sav = ((ExecOperationsReplySAV*)f);
		sav->send(reply);
		sav->delref();
	} catch (Error& e) {
		fprintf(stderr, "fdb_rpc_client_exec_request: Unexpected FDB error %d\n", e.code());
		abort();
	} catch (...) {
		fprintf(stderr, "fdb_rpc_client_exec_request: Unexpected FDB unknown error\n");
		abort();
	}
}

void DLRPCClient_sendError(FDBFuture* f, fdb_error_t err) {
	ExecOperationsReplySAV* sav = ((ExecOperationsReplySAV*)f);
	sav->sendError(Error(err));
	sav->delref();
}

DLRPCClient::DLRPCClient(std::string connFilename) {
	fdb_error_t err = fdb_rpc_client_create(connFilename.c_str(), DLRPCClient_sendReply, DLRPCClient_sendError, &impl);
	if (err != error_code_success) {
		throw Error(err);
	}
}

DLRPCClient::~DLRPCClient() {
	fdb_rpc_client_destroy(impl);
}

void DLRPCClient::executeOperations(ExecOperationsReference request, ExecOperationsReplySAV* result) {
	ObjectWriter ow(Unversioned());
	ow.serialize(*(ExecOperationsReqInput*)request.getPtr());
	StringRef str = ow.toStringRef();
	fdb_rpc_client_exec_request(impl, str.begin(), str.size(), (FDBFuture*)result);
}

void DLRPCClient::releaseTransaction(uint64_t transaction) {
	fdb_rpc_client_release_transaction(impl, transaction);
}

extern "C" DLLEXPORT fdb_error_t fdb_rpc_client_create(const char* connFilename,
                                                       SendReplyCallback replyCB,
                                                       SendErrorCallback errorCB,
                                                       EmbeddedRPCClient** client) {
	try {
		*client = new ExternalRPCClient(connFilename, replyCB, errorCB);
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
		client->executeOperations(ref, (ExecOperationsReplySAV*)result);
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
