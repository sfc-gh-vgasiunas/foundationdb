/*
 * ClientProxyStub.h
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

#ifndef FDBCLIENT_CLIENTPROXYSTUB_H
#define FDBCLIENT_CLIENTPROXYSTUB_H
#pragma once

#include "fdbclient/IClientApi.h"
#include "fdbclient/ClientProxyInterface.h"
#include <mutex>
#include <atomic>

struct ExecOperationsRequestRefCounted : public ClientProxy::ExecOperationsRequest,
                                         public ThreadSafeReferenceCounted<ExecOperationsRequestRefCounted> {};

struct ClientProxyInterfaceRefCounted : public ClientProxyInterface,
                                        public ThreadSafeReferenceCounted<ClientProxyInterfaceRefCounted> {};

// An implementation of IDatabase that forwards API calls for execution on a client proxy
class ClientProxyDatabaseStub : public IDatabase, public ThreadSafeReferenceCounted<ClientProxyDatabaseStub> {
public:
	~ClientProxyDatabaseStub() override;
	Reference<ITransaction> createTransaction() override;
	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	double getMainThreadBusyness() override;
	ThreadFuture<ProtocolVersion> getServerProtocol(
	    Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>()) override;
	void addref() override { ThreadSafeReferenceCounted<ClientProxyDatabaseStub>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<ClientProxyDatabaseStub>::delref(); }

	ThreadFuture<int64_t> rebootWorker(const StringRef& address, bool check, int duration) override;
	ThreadFuture<Void> forceRecoveryWithDataLoss(const StringRef& dcid) override;
	ThreadFuture<Void> createSnapshot(const StringRef& uid, const StringRef& snapshot_command) override;

private:
	friend class ClientProxyTransactionStub;
	friend class ClientProxyAPIStub;
	// Internal use only
	ClientProxyDatabaseStub(std::string proxyUrl, int apiVersion);

	Reference<ClientProxyInterfaceRefCounted> interface;
	uint64_t clientID;
	std::atomic<uint64_t> txCounter;
};

// An implementation of ITransaction that forwards API calls for execution on a client proxy
class ClientProxyTransactionStub : public ITransaction,
                                   ThreadSafeReferenceCounted<ClientProxyTransactionStub>,
                                   NonCopyable {
public:
	explicit ClientProxyTransactionStub(ClientProxyDatabaseStub* db, uint64_t txID);
	~ClientProxyTransactionStub() override;

	void cancel() override;
	void setVersion(Version v) override;
	ThreadFuture<Version> getReadVersion() override;

	ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot = false) override;
	ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot = false) override;
	ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                   const KeySelectorRef& end,
	                                   int limit,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                   const KeySelectorRef& end,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   int limit,
	                                   bool snapshot = false,
	                                   bool reverse = false) override {
		return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limit, snapshot, reverse);
	}
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override {
		return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limits, snapshot, reverse);
	}
	ThreadFuture<RangeResult> getRangeAndFlatMap(const KeySelectorRef& begin,
	                                             const KeySelectorRef& end,
	                                             const StringRef& mapper,
	                                             GetRangeLimits limits,
	                                             bool snapshot,
	                                             bool reverse) override;
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) override;
	ThreadFuture<Standalone<StringRef>> getVersionstamp() override;
	ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) override;
	ThreadFuture<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRangeRef& range,
	                                                                int64_t chunkSize) override;
	ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRangeRef& keyRange) override;
	ThreadResult<RangeResult> readBlobGranules(const KeyRangeRef& keyRange,
	                                           Version beginVersion,
	                                           Optional<Version> readVersion,
	                                           ReadBlobGranuleContext granuleContext) override;
	void addReadConflictRange(const KeyRangeRef& keys) override;
	void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) override;
	void set(const KeyRef& key, const ValueRef& value) override;
	void clear(const KeyRef& begin, const KeyRef& end) override;
	void clear(const KeyRangeRef& range) override;
	void clear(const KeyRef& key) override;
	ThreadFuture<Void> watch(const KeyRef& key) override;
	void addWriteConflictRange(const KeyRangeRef& keys) override;
	ThreadFuture<Void> commit() override;
	Version getCommittedVersion() override;
	ThreadFuture<int64_t> getApproximateSize() override;
	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	ThreadFuture<Void> onError(Error const& e) override;
	void reset() override;

	void addref() override { ThreadSafeReferenceCounted<ClientProxyTransactionStub>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<ClientProxyTransactionStub>::delref(); }

private:
	void createExecRequest();
	void addOperation(const ClientProxy::Operation& op);

	template <class ResType>
	ThreadFuture<typename ResType::value_type> sendCurrExecRequest();

	std::mutex mutex;
	Reference<ExecOperationsRequestRefCounted> currExecRequest;
	Reference<ClientProxyDatabaseStub> db;
	uint64_t transactionID;
	int32_t operationCounter;
};

// An implementation of IClientApi that serializes operations onto the network thread and interacts with the lower-level
// client APIs exposed by NativeAPI and ReadYourWrites.
class ClientProxyAPIStub : public IClientApi, ThreadSafeReferenceCounted<ClientProxyAPIStub> {
public:
	void selectApiVersion(int apiVersion) override;
	const char* getClientVersion() override;
	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	void setupNetwork() override;
	void runNetwork() override;
	void stopNetwork() override;
	Reference<IDatabase> createDatabase(const char* clusterFilePath) override;
	void addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) override;

private:
	friend IClientApi* getClientProxyAPI();
	ClientProxyAPIStub();

	int apiVersion;
	std::string proxyUrl;
};

#endif
