/*
 * ClientProxyInterface.h
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
#ifndef FDBCLIENT_CLIENTPROXYINTERFACE_H
#define FDBCLIENT_CLIENTPROXYINTERFACE_H

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

namespace ClientProxy {

struct GetOp {
	constexpr static FileIdentifier file_identifier = 5789861;

	KeyRef key;
	bool snapshot;

	GetOp() {}
	GetOp(Arena& ar, KeyRef key, bool snapshot) : key(ar, key), snapshot(snapshot) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, snapshot);
	}
};

struct GetRangeOp {
	constexpr static FileIdentifier file_identifier = 5789862;

	KeySelectorRef begin;
	KeySelectorRef end;
	GetRangeLimits limits;
	bool snapshot;
	bool reverse;

	GetRangeOp() {}
	GetRangeOp(Arena& ar, KeySelectorRef begin, KeySelectorRef end, GetRangeLimits limits, bool snapshot, bool reverse)
	  : begin(ar, begin), end(ar, end), limits(limits), snapshot(snapshot), reverse(reverse) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, begin, end, limits, snapshot, reverse);
	}
};

struct SetOp {
	constexpr static FileIdentifier file_identifier = 5789863;

	KeyRef key;
	ValueRef value;

	SetOp() {}
	SetOp(Arena& ar, KeyRef key, ValueRef value) : key(ar, key), value(ar, value) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value);
	}
};

struct CommitOp {
	constexpr static FileIdentifier file_identifier = 5789864;

	template <class Ar>
	void serialize(Ar& ar) {}
};

struct SetOptionOp {
	constexpr static FileIdentifier file_identifier = 5789865;

	int optionId;
	Optional<ValueRef> value;

	SetOptionOp() {}
	SetOptionOp(Arena& ar, int optionId, Optional<ValueRef> value) : optionId(optionId), value(ar, value) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, optionId, value);
	}
};

struct ResetOp {
	constexpr static FileIdentifier file_identifier = 5789866;

	template <class Ar>
	void serialize(Ar& ar) {}
};

struct ClearOp {
	constexpr static FileIdentifier file_identifier = 5789867;

	KeyRef key;

	ClearOp() {}
	ClearOp(Arena& ar, KeyRef key) : key(ar, key) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key);
	}
};

struct ClearRangeOp {
	constexpr static FileIdentifier file_identifier = 5789868;

	KeyRef begin;
	KeyRef end;

	ClearRangeOp() {}
	ClearRangeOp(Arena& ar, KeyRef begin, KeyRef end) : begin(ar, begin), end(ar, end) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, begin, end);
	}
};

struct GetReadVersionOp {
	constexpr static FileIdentifier file_identifier = 5789869;

	template <class Ar>
	void serialize(Ar& ar) {}
};

struct AddReadConflictRangeOp {
	constexpr static FileIdentifier file_identifier = 5789870;

	KeyRangeRef range;

	AddReadConflictRangeOp() {}
	AddReadConflictRangeOp(Arena& ar, KeyRangeRef range) : range(ar, range) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range);
	}
};

struct OnErrorOp {
	constexpr static FileIdentifier file_identifier = 5789871;

	int errorCode;

	OnErrorOp() {}
	OnErrorOp(int errorCode) : errorCode(errorCode) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, errorCode);
	}
};

using Operation = std::variant<GetOp,
                               GetRangeOp,
                               SetOp,
                               CommitOp,
                               SetOptionOp,
                               ResetOp,
                               ClearOp,
                               ClearRangeOp,
                               GetReadVersionOp,
                               AddReadConflictRangeOp,
                               OnErrorOp>;

enum OperationType {
	OP_GET,
	OP_GETRANGE,
	OP_SET,
	OP_COMMIT,
	OP_SETOPTION,
	OP_RESET,
	OP_CLEAR,
	OP_CLEARRANGE,
	OP_GETREADVERSION,
	OP_ADDREADCONFLICTRANGE,
	OP_ONERROR
};

template <class ValT, int file_id>
struct ResultTypeBase {
	constexpr static FileIdentifier file_identifier = file_id;
	using value_type = ValT;
	value_type val;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, val);
	}
};

struct VoidResult : public ResultTypeBase<Void, 9572646> {};
struct GetResult : public ResultTypeBase<Optional<Value>, 9572647> {};
struct GetRangeResult : public ResultTypeBase<RangeResult, 9572648> {};
struct Int64Result : public ResultTypeBase<int64_t, 9572649> {};

using OperationResult = std::variant<VoidResult, GetResult, GetRangeResult, Int64Result>;

struct ExecOperationsReply {
	constexpr static FileIdentifier file_identifier = 1534762;

	OperationResult res;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, res);
	}
};

struct ExecOperationsRequest {
	constexpr static FileIdentifier file_identifier = 7632546;
	Arena arena;
	uint64_t clientID;
	uint64_t transactionID;
	uint32_t firstSeqNo;
	std::vector<Operation> operations;
	std::vector<uint64_t> releasedTransactions;
	ReplyPromise<ExecOperationsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clientID, transactionID, firstSeqNo, operations, releasedTransactions, reply, arena);
	}
};

} // namespace ClientProxy

struct ClientProxyInterface {
	constexpr static FileIdentifier file_identifier = 6483623;
	RequestStream<ClientProxy::ExecOperationsRequest> execOperations;
	LocalityData locality;
	UID uniqueID;

	ClientProxyInterface() {}
	explicit ClientProxyInterface(const LocalityData& locality)
	  : locality(locality), uniqueID(deterministicRandom()->randomUniqueID()) {}
	void initClientEndpoints(NetworkAddress remote);
	void initServerEndpoints();

	UID id() const { return uniqueID; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, locality, uniqueID, execOperations);
	}
};

#endif