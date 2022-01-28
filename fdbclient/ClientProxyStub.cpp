/*
 * ClientProxyStub.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ClientProxyStub.h"
#include "flow/IRandom.h"
#include "fdbclient/EmbeddedRPCClient.h"

using namespace ClientProxy;

#define UNIMPLEMENTED_OPERATION()                                                                                      \
	std::cout << "Unimplemented proxy operation called: " << __func__ << std::endl;                                    \
	std::cout.flush();                                                                                                 \
	throw unsupported_operation();

Reference<ITransaction> ClientProxyDatabaseStub::createTransaction() {
	// std::cout << "Creating proxy transaction " << std::endl;
	return Reference<ITransaction>(new ClientProxyTransactionStub(this, txCounter++));
}

void ClientProxyDatabaseStub::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<int64_t> ClientProxyDatabaseStub::rebootWorker(const StringRef& address, bool check, int duration) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Void> ClientProxyDatabaseStub::forceRecoveryWithDataLoss(const StringRef& dcid) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Void> ClientProxyDatabaseStub::createSnapshot(const StringRef& uid, const StringRef& snapshot_command) {
	UNIMPLEMENTED_OPERATION();
}

// Return the main network thread busyness
double ClientProxyDatabaseStub::getMainThreadBusyness() {
	ASSERT(g_network);
	return g_network->networkInfo.metrics.networkBusyness;
}

// Returns the protocol version reported by the coordinator this client is connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
ThreadFuture<ProtocolVersion> ClientProxyDatabaseStub::getServerProtocol(Optional<ProtocolVersion> expectedVersion) {
	UNIMPLEMENTED_OPERATION();
}

ClientProxyDatabaseStub::ClientProxyDatabaseStub(Reference<IClientRPCInterface> rpcInterface, int apiVersion)
  : rpcInterface(rpcInterface), clientID(rpcInterface->getClientID()), txCounter(0) {}

ClientProxyDatabaseStub::~ClientProxyDatabaseStub() {
	std::cout << "Destroying proxy database" << std::endl;
}

ClientProxyTransactionStub::ClientProxyTransactionStub(ClientProxyDatabaseStub* db, uint64_t txID)
  : db(Reference<ClientProxyDatabaseStub>::addRef(db)), transactionID(txID), operationCounter(0),
    committedVersion(invalidVersion) {}

ClientProxyTransactionStub::~ClientProxyTransactionStub() {
	db->getRpcInterface()->releaseTransaction(transactionID);
}

void ClientProxyTransactionStub::createExecRequest() {
	if (!currExecRequest.isValid()) {
		// std::cout << "Creating exec operations request " << std::endl;
		currExecRequest = makeReference<ExecOperationsRequestRefCounted>();
		currExecRequest->clientID = db->clientID;
		currExecRequest->transactionID = transactionID;
		currExecRequest->firstSeqNo = operationCounter;
	}
}

void ClientProxyTransactionStub::addOperation(const Operation& op) {
	currExecRequest->operations.push_back(op);
	operationCounter++;
}

class ExecOperationsCallbackBase : public IExecOperationsCallback {
public:
	ExecOperationsCallbackBase(ThreadSingleAssignmentVarBase* sav) : sav(sav), proxyRequest(nullptr) { sav->addref(); }
	void sendError(const Error& e) override {
		sav->sendError(e);
		sav->delref();
	}
	void setProxyRequest(ClientProxy::ProxyRequestState* proxyRequest) override { this->proxyRequest = proxyRequest; }
	ClientProxy::ProxyRequestState* getProxyRequest() override { return proxyRequest; }
	void setCancel(Future<Void>&& cancel) override { sav->setCancel(std::move(cancel)); }
	void cancelProxyRequest() override { clientIfc->cancelRequest(proxyRequest); }
	void setClientInterface(Reference<IClientRPCInterface> clientIfc) { this->clientIfc = clientIfc; }

	ThreadSingleAssignmentVarBase* sav;
	ClientProxy::ProxyRequestState* proxyRequest;
	Reference<IClientRPCInterface> clientIfc;
};

template <class T>
class ExecOperationsCallbackSAV : public ThreadSingleAssignmentVar<T> {
public:
	ExecOperationsCallbackSAV(IExecOperationsCallback* cb) : cb(cb) {}
	~ExecOperationsCallbackSAV() override { delete (cb); }

	void cancel() override {
		cb->cancelProxyRequest();
		ThreadSingleAssignmentVar<T>::cancel();
	}

private:
	IExecOperationsCallback* cb;
};

template <class ResType>
class ExecOperationsExtractValueCallback : public ExecOperationsCallbackBase {
public:
	using SAVType = ExecOperationsCallbackSAV<typename ResType::value_type>;

	ExecOperationsExtractValueCallback() : ExecOperationsCallbackBase(new SAVType(this)) {}

	SAVType* getSAV() { return (SAVType*)this->sav; }

	void sendReply(const ExecOperationsReply& reply) override {
		getSAV()->send(std::get<ResType>(reply.res).val);
		getSAV()->delref();
	}
};

class ExecOperationsCommitCallback : public ExecOperationsCallbackBase {
public:
	using SAVType = ExecOperationsCallbackSAV<Void>;

	ExecOperationsCommitCallback(Reference<ClientProxyTransactionStub> tx)
	  : ExecOperationsCallbackBase(new SAVType(this)), tx(tx) {}

	SAVType* getSAV() { return (SAVType*)this->sav; }

	void sendReply(const ExecOperationsReply& reply) override {
		tx->setCommittedVersion(std::get<Int64Result>(reply.res).val);
		getSAV()->send(Void());
		getSAV()->delref();
	}

	Reference<ClientProxyTransactionStub> tx;
};

void ClientProxyTransactionStub::sendCurrExecRequest(ExecOperationsCallbackBase* cb) {
	ASSERT(currExecRequest.isValid());
	Reference<ExecOperationsRequestRefCounted> request = this->currExecRequest;
	currExecRequest.clear();
	cb->clientIfc = db->getRpcInterface();
	db->getRpcInterface()->executeOperations(request, cb);
}

template <class ResType>
ThreadFuture<typename ResType::value_type> ClientProxyTransactionStub::sendAndGetValue() {
	ExecOperationsExtractValueCallback<ResType>* cb = new ExecOperationsExtractValueCallback<ResType>();
	sendCurrExecRequest(cb);
	return ThreadFuture<typename ResType::value_type>(cb->getSAV());
}

void ClientProxyTransactionStub::cancel() {
	UNIMPLEMENTED_OPERATION();
}

void ClientProxyTransactionStub::setVersion(Version v) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Version> ClientProxyTransactionStub::getReadVersion() {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(GetReadVersionOp()));
	return sendAndGetValue<Int64Result>();
}

ThreadFuture<Optional<Value>> ClientProxyTransactionStub::get(const KeyRef& key, bool snapshot) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(GetOp(currExecRequest->arena, key, snapshot)));
	return sendAndGetValue<GetResult>();
}

ThreadFuture<Key> ClientProxyTransactionStub::getKey(const KeySelectorRef& key, bool snapshot) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<int64_t> ClientProxyTransactionStub::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> ClientProxyTransactionStub::getRangeSplitPoints(const KeyRangeRef& range,
                                                                                            int64_t chunkSize) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> ClientProxyTransactionStub::getBlobGranuleRanges(
    const KeyRangeRef& keyRange) {
	UNIMPLEMENTED_OPERATION();
}

ThreadResult<RangeResult> ClientProxyTransactionStub::readBlobGranules(const KeyRangeRef& keyRange,
                                                                       Version beginVersion,
                                                                       Optional<Version> readVersion,
                                                                       ReadBlobGranuleContext granuleContext) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<RangeResult> ClientProxyTransactionStub::getRange(const KeySelectorRef& begin,
                                                               const KeySelectorRef& end,
                                                               int limit,
                                                               bool snapshot,
                                                               bool reverse) {
	return getRange(begin, end, GetRangeLimits(limit), snapshot, reverse);
}

ThreadFuture<RangeResult> ClientProxyTransactionStub::getRange(const KeySelectorRef& begin,
                                                               const KeySelectorRef& end,
                                                               GetRangeLimits limits,
                                                               bool snapshot,
                                                               bool reverse) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(GetRangeOp(currExecRequest->arena, begin, end, limits, snapshot, reverse)));
	return sendAndGetValue<GetRangeResult>();
}

ThreadFuture<RangeResult> ClientProxyTransactionStub::getRangeAndFlatMap(const KeySelectorRef& begin,
                                                                         const KeySelectorRef& end,
                                                                         const StringRef& mapper,
                                                                         GetRangeLimits limits,
                                                                         bool snapshot,
                                                                         bool reverse) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Standalone<VectorRef<const char*>>> ClientProxyTransactionStub::getAddressesForKey(const KeyRef& key) {
	UNIMPLEMENTED_OPERATION();
}

void ClientProxyTransactionStub::addReadConflictRange(const KeyRangeRef& keys) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(AddReadConflictRangeOp(currExecRequest->arena, keys)));
}

void ClientProxyTransactionStub::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	UNIMPLEMENTED_OPERATION();
}

void ClientProxyTransactionStub::set(const KeyRef& key, const ValueRef& value) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(SetOp(currExecRequest->arena, key, value)));
}

void ClientProxyTransactionStub::clear(const KeyRangeRef& range) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(ClearRangeOp(currExecRequest->arena, range.begin, range.end)));
}

void ClientProxyTransactionStub::clear(const KeyRef& begin, const KeyRef& end) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(ClearRangeOp(currExecRequest->arena, begin, end)));
}

void ClientProxyTransactionStub::clear(const KeyRef& key) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(ClearOp(currExecRequest->arena, key)));
}

ThreadFuture<Void> ClientProxyTransactionStub::watch(const KeyRef& key) {
	UNIMPLEMENTED_OPERATION();
}

void ClientProxyTransactionStub::addWriteConflictRange(const KeyRangeRef& keys) {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Void> ClientProxyTransactionStub::commit() {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(CommitOp()));
	ExecOperationsCommitCallback* cb =
	    new ExecOperationsCommitCallback(Reference<ClientProxyTransactionStub>::addRef(this));
	sendCurrExecRequest(cb);
	return ThreadFuture<Void>(cb->getSAV());
}

Version ClientProxyTransactionStub::getCommittedVersion() {
	return committedVersion;
}

ThreadFuture<int64_t> ClientProxyTransactionStub::getApproximateSize() {
	UNIMPLEMENTED_OPERATION();
}

ThreadFuture<Standalone<StringRef>> ClientProxyTransactionStub::getVersionstamp() {
	UNIMPLEMENTED_OPERATION();
}

void ClientProxyTransactionStub::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(SetOptionOp(currExecRequest->arena, option, value)));
}

ThreadFuture<Void> ClientProxyTransactionStub::onError(Error const& e) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(OnErrorOp(e.code())));
	return sendAndGetValue<VoidResult>();
}

void ClientProxyTransactionStub::reset() {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(ResetOp()));
}

extern const char* getSourceVersion();

ClientProxyAPIStub::ClientProxyAPIStub() : apiVersion(-1) {}

void ClientProxyAPIStub::selectApiVersion(int apiVersion) {
	this->apiVersion = apiVersion;
}

const char* ClientProxyAPIStub::getClientVersion() {
	return "proxy";
}

void ClientProxyAPIStub::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if (option == FDBNetworkOptions::PROXY_URL) {
		if (value.present()) {
			proxyUrl = value.get().toString();
			if (proxyUrl == "embedded") {
				embeddedProxy = true;
				proxyUrl.clear();
			}
		}
	}
}

void ClientProxyAPIStub::setupNetwork() {
	UNIMPLEMENTED_OPERATION();
}

void ClientProxyAPIStub::runNetwork() {
	UNIMPLEMENTED_OPERATION();
}

void ClientProxyAPIStub::stopNetwork() {
	UNIMPLEMENTED_OPERATION();
}

Reference<IDatabase> ClientProxyAPIStub::createDatabase(const char* clusterFilePath) {
	Reference<IClientRPCInterface> rpcIfc;
	if (embeddedProxy) {
		rpcIfc = makeReference<EmbeddedRPCClient>(clusterFilePath);
	} else {
		rpcIfc = makeReference<ClientProxyRPCStub>(proxyUrl);
	}
	return Reference<IDatabase>(new ClientProxyDatabaseStub(rpcIfc, apiVersion));
}

Reference<IDatabase> ClientProxyAPIStub::createDLProxyDatabase(const char* clusterFile,
                                                               Reference<FDBProxyCApi> proxyApi) {
	Reference<IClientRPCInterface> rpcIfc = makeReference<DLRPCClient>(clusterFile, proxyApi);
	return Reference<IDatabase>(new ClientProxyDatabaseStub(rpcIfc, apiVersion));
}

void ClientProxyAPIStub::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	UNIMPLEMENTED_OPERATION();
}
