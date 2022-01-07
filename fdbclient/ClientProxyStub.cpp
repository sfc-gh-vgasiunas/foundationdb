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

using namespace ClientProxy;

ThreadFuture<ExecOperationsReply> sendExecRequest(Reference<ClientProxyInterfaceRefCounted> ifc,
                                                  Reference<ExecOperationsRequestRefCounted> request) {
	return onMainThread([ifc, request]() { return ifc->execOperations.getReply(*request); });
}

Reference<ITransaction> ClientProxyDatabaseStub::createTransaction() {
	std::cout << "Creating proxy transaction " << std::endl;
	return Reference<ITransaction>(new ClientProxyTransactionStub(this, txCounter++));
}

void ClientProxyDatabaseStub::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	throw unsupported_operation();
}

ThreadFuture<int64_t> ClientProxyDatabaseStub::rebootWorker(const StringRef& address, bool check, int duration) {
	throw unsupported_operation();
}

ThreadFuture<Void> ClientProxyDatabaseStub::forceRecoveryWithDataLoss(const StringRef& dcid) {
	throw unsupported_operation();
}

ThreadFuture<Void> ClientProxyDatabaseStub::createSnapshot(const StringRef& uid, const StringRef& snapshot_command) {
	throw unsupported_operation();
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
	throw unsupported_operation();
}

ClientProxyDatabaseStub::ClientProxyDatabaseStub(std::string proxyUrl, int apiVersion) {
	std::cout << "Connecting to proxy " << proxyUrl << std::endl;
	NetworkAddress proxyAddress = NetworkAddress::parse(proxyUrl);
	interface = makeReference<ClientProxyInterfaceRefCounted>();
	interface->initClientEndpoints(proxyAddress);
	clientID = deterministicRandom()->randomUInt64();
	txCounter = 0;
}

ClientProxyDatabaseStub::~ClientProxyDatabaseStub() {
	std::cout << "Destroying proxy database" << std::endl;
}

ClientProxyTransactionStub::ClientProxyTransactionStub(ClientProxyDatabaseStub* db, uint64_t txID)
  : db(Reference<ClientProxyDatabaseStub>::addRef(db)), transactionID(txID), operationCounter(0) {}

ClientProxyTransactionStub::~ClientProxyTransactionStub() {
	std::cout << "Destroying proxy transaction" << std::endl;
}

void ClientProxyTransactionStub::createExecRequest() {
	if (!currExecRequest.isValid()) {
		std::cout << "Creating exec operations request " << std::endl;
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

template <class ResType>
ThreadFuture<typename ResType::value_type> ClientProxyTransactionStub::sendCurrExecRequest() {
	using ValType = typename ResType::value_type;
	ASSERT(currExecRequest.isValid());
	Reference<ExecOperationsRequestRefCounted> request = this->currExecRequest;
	currExecRequest.clear();
	Reference<ClientProxyInterfaceRefCounted> ifc = this->db->interface;
	ThreadFuture<ExecOperationsReply> replyFuture = sendExecRequest(ifc, request);

	return mapThreadFuture<ExecOperationsReply, ValType>(replyFuture, [](ErrorOr<ExecOperationsReply> reply) {
		if (reply.isError()) {
			return ErrorOr<ValType>(reply.getError());
		} else {
			return ErrorOr<ValType>(std::get<ResType>(reply.get().res).val);
		}
	});
}

void ClientProxyTransactionStub::cancel() {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::setVersion(Version v) {
	throw unsupported_operation();
}

ThreadFuture<Version> ClientProxyTransactionStub::getReadVersion() {
	throw unsupported_operation();
}

ThreadFuture<Optional<Value>> ClientProxyTransactionStub::get(const KeyRef& key, bool snapshot) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(GetOp(currExecRequest->arena, key, snapshot)));
	return sendCurrExecRequest<GetResult>();
}

ThreadFuture<Key> ClientProxyTransactionStub::getKey(const KeySelectorRef& key, bool snapshot) {
	throw unsupported_operation();
}

ThreadFuture<int64_t> ClientProxyTransactionStub::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	throw unsupported_operation();
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> ClientProxyTransactionStub::getRangeSplitPoints(const KeyRangeRef& range,
                                                                                            int64_t chunkSize) {
	throw unsupported_operation();
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> ClientProxyTransactionStub::getBlobGranuleRanges(
    const KeyRangeRef& keyRange) {
	throw unsupported_operation();
}

ThreadResult<RangeResult> ClientProxyTransactionStub::readBlobGranules(const KeyRangeRef& keyRange,
                                                                       Version beginVersion,
                                                                       Optional<Version> readVersion,
                                                                       ReadBlobGranuleContext granuleContext) {
	throw unsupported_operation();
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
	return sendCurrExecRequest<GetRangeResult>();
}

ThreadFuture<RangeResult> ClientProxyTransactionStub::getRangeAndFlatMap(const KeySelectorRef& begin,
                                                                         const KeySelectorRef& end,
                                                                         const StringRef& mapper,
                                                                         GetRangeLimits limits,
                                                                         bool snapshot,
                                                                         bool reverse) {
	throw unsupported_operation();
}

ThreadFuture<Standalone<VectorRef<const char*>>> ClientProxyTransactionStub::getAddressesForKey(const KeyRef& key) {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::addReadConflictRange(const KeyRangeRef& keys) {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::set(const KeyRef& key, const ValueRef& value) {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(SetOp(currExecRequest->arena, key, value)));
}

void ClientProxyTransactionStub::clear(const KeyRangeRef& range) {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::clear(const KeyRef& begin, const KeyRef& end) {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::clear(const KeyRef& key) {
	throw unsupported_operation();
}

ThreadFuture<Void> ClientProxyTransactionStub::watch(const KeyRef& key) {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::addWriteConflictRange(const KeyRangeRef& keys) {
	throw unsupported_operation();
}

ThreadFuture<Void> ClientProxyTransactionStub::commit() {
	std::unique_lock<std::mutex> l(mutex);
	createExecRequest();
	addOperation(Operation(CommitOp()));
	return sendCurrExecRequest<VoidResult>();
}

Version ClientProxyTransactionStub::getCommittedVersion() {
	throw unsupported_operation();
}

ThreadFuture<int64_t> ClientProxyTransactionStub::getApproximateSize() {
	throw unsupported_operation();
}

ThreadFuture<Standalone<StringRef>> ClientProxyTransactionStub::getVersionstamp() {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	throw unsupported_operation();
}

ThreadFuture<Void> ClientProxyTransactionStub::onError(Error const& e) {
	throw unsupported_operation();
}

void ClientProxyTransactionStub::reset() {
	throw unsupported_operation();
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
		}
	}
}

void ClientProxyAPIStub::setupNetwork() {
	throw unsupported_operation();
}

void ClientProxyAPIStub::runNetwork() {
	throw unsupported_operation();
}

void ClientProxyAPIStub::stopNetwork() {
	throw unsupported_operation();
}

Reference<IDatabase> ClientProxyAPIStub::createDatabase(const char* clusterFilePath) {
	return Reference<IDatabase>(new ClientProxyDatabaseStub(proxyUrl, apiVersion));
}

void ClientProxyAPIStub::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	throw unsupported_operation();
}
