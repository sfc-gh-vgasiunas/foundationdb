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

/* --------------------------------------------------------------------------------
 * Remote proxy stub
 * --------------------------------------------------------------------------------- */

namespace {

ACTOR
Future<Void> executeRemoteProxyOperationsActor(Future<Void> signal,
                                               Reference<ClientProxyRPCStub> client,
                                               ExecOperationsReference request,
                                               IExecOperationsCallback* result) {
	try {
		wait(signal);
		while (true) {
			Optional<uint64_t> t = client->releasedTransactions.pop();
			if (!t.present())
				break;
			request->releasedTransactions.push_back(t.get());
		}
		ExecOperationsReply r = wait(client->interface.execOperations.getReply(*request));
		result->sendReply(r);
	} catch (Error& e) {
		result->sendError(e);
	}
	return Void();
}

} // namespace

ClientProxyRPCStub::ClientProxyRPCStub(std::string proxyUrl) {
	std::cout << "Connecting to proxy " << proxyUrl << std::endl;
	NetworkAddress proxyAddress = NetworkAddress::parse(proxyUrl);
	interface.initClientEndpoints(proxyAddress);
	clientID = deterministicRandom()->randomUInt64();
}

void ClientProxyRPCStub::executeOperations(ExecOperationsReference request, IExecOperationsCallback* result) {
	Reference<ClientProxyRPCStub> thisRef = Reference<ClientProxyRPCStub>::addRef(this);
	Promise<Void> signal;
	Future<Void> cancelFuture = executeRemoteProxyOperationsActor(signal.getFuture(), thisRef, request, result);
	result->setCancel(std::move(cancelFuture));
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
}

void ClientProxyRPCStub::releaseTransaction(uint64_t transactionID) {
	releasedTransactions.push(transactionID);
}

/* --------------------------------------------------------------------------------
 * Embedded local proxy stub
 * --------------------------------------------------------------------------------- */

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

void EmbeddedRPCClient::executeOperations(ExecOperationsReference request, IExecOperationsCallback* result) {
	Reference<EmbeddedRPCClient> client = Reference<EmbeddedRPCClient>::addRef(this);
	onMainThreadVoid([client, request, result]() {
		while (true) {
			Optional<uint64_t> t = client->releasedTransactions.pop();
			if (!t.present())
				break;
			ClientProxy::releaseTransaction(client->proxyState, UID(EmbeddedRPCClient::CLIENT_ID, t.get()));
		}
		ProxyRequestReference proxyReq =
		    handleExecOperationsRequest(client->proxyState, request, client.getPtr(), result);
		client->setProxyRequest(result, proxyReq.extractPtr());
	});
}

void EmbeddedRPCClient::releaseTransaction(uint64_t transaction) {
	releasedTransactions.push(transaction);
}

void EmbeddedRPCClient::sendReply(const ExecOperationsReply& reply, IExecOperationsCallback* result) {
	result->sendReply(reply);
}

void EmbeddedRPCClient::sendError(const Error& e, IExecOperationsCallback* result) {
	result->sendError(e);
}

void EmbeddedRPCClient::cancelRequest(ProxyRequestState* proxyRequest) {
	onMainThreadVoid([proxyRequest]() { proxyRequest->cancel(); });
}

void EmbeddedRPCClient::setProxyRequest(IExecOperationsCallback* callback, ProxyRequestState* proxyRequest) {
	callback->setProxyRequest(proxyRequest);
}

/* ------------------------------------------------x--------------------------------
 * External local proxy stub
 * --------------------------------------------------------------------------------- */

void ExternalRPCClient::sendReply(const ExecOperationsReply& reply, IExecOperationsCallback* result) {
	ObjectWriter ow(Unversioned());
	ow.serialize(reply);
	StringRef str = ow.toStringRef();
	sendReplyCB((FDBFuture*)result, str.begin(), str.size());
}

void ExternalRPCClient::sendError(const Error& e, IExecOperationsCallback* result) {
	sendErrorCB((FDBFuture*)result, e.code());
}

void ExternalRPCClient::setProxyRequest(IExecOperationsCallback* callback, ProxyRequestState* proxyReq) {
	setProxyReqCB((FDBFuture*)callback, proxyReq);
}

void DLRPCClient_sendReply(FDBFuture* f, const void* bytes, int size) {
	try {
		ObjectReader rd((const uint8_t*)bytes, Unversioned());
		ExecOperationsReply reply;
		rd.deserialize(reply);
		IExecOperationsCallback* sav = ((IExecOperationsCallback*)f);
		sav->sendReply(reply);
	} catch (Error& e) {
		fprintf(stderr, "fdb_rpc_client_exec_request: Unexpected FDB error %d\n", e.code());
		abort();
	} catch (...) {
		fprintf(stderr, "fdb_rpc_client_exec_request: Unexpected FDB unknown error\n");
		abort();
	}
}

void DLRPCClient_sendError(FDBFuture* f, fdb_error_t err) {
	IExecOperationsCallback* sav = ((IExecOperationsCallback*)f);
	sav->sendError(Error(err));
}

void DLRPCClient_setProxyRequest(FDBFuture* f, ClientProxy::ProxyRequestState* proxyRequest) {
	IExecOperationsCallback* sav = ((IExecOperationsCallback*)f);
	sav->setProxyRequest(proxyRequest);
}

DLRPCClient::DLRPCClient(std::string connFilename) {
	fdb_error_t err = fdb_rpc_client_create(
	    connFilename.c_str(), DLRPCClient_sendReply, DLRPCClient_sendError, DLRPCClient_setProxyRequest, &impl);
	if (err != error_code_success) {
		throw Error(err);
	}
}

DLRPCClient::~DLRPCClient() {
	fdb_rpc_client_destroy(impl);
}

void DLRPCClient::executeOperations(ExecOperationsReference request, IExecOperationsCallback* result) {
	ObjectWriter ow(Unversioned());
	ow.serialize(*(ExecOperationsReqInput*)request.getPtr());
	StringRef str = ow.toStringRef();
	fdb_rpc_client_exec_request(impl, str.begin(), str.size(), (FDBFuture*)result);
}

void DLRPCClient::releaseTransaction(uint64_t transaction) {
	fdb_rpc_client_release_transaction(impl, transaction);
}

void DLRPCClient::cancelRequest(ClientProxy::ProxyRequestState* proxyRequest) {
	fdb_rpc_client_cancel_request(impl, proxyRequest);
}

extern "C" DLLEXPORT fdb_error_t fdb_rpc_client_create(const char* connFilename,
                                                       SendReplyCallback replyCB,
                                                       SendErrorCallback errorCB,
                                                       SetProxyRequestCallback setProxyReqCB,
                                                       EmbeddedRPCClient** client) {
	try {
		*client = new ExternalRPCClient(connFilename, replyCB, errorCB, setProxyReqCB);
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
		client->executeOperations(ref, (IExecOperationsCallback*)result);
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

extern "C" DLLEXPORT void fdb_rpc_client_cancel_request(EmbeddedRPCClient* client, ProxyRequestState* proxyRequest) {
	client->cancelRequest(proxyRequest);
}
