/*
 * ClientProxy.actor.cpp
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
#include "fdbclient/ClientProxy.actor.h"
#include "fdbclient/WellKnownEndpoints.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

void ClientProxyInterface::initClientEndpoints(NetworkAddress remote) {
	execOperations = RequestStream<ClientProxy::ExecOperationsRequest>(
	    Endpoint::wellKnown({ remote }, WLTOKEN_CLIENTPROXY_EXECOPERATIONS));
}

void ClientProxyInterface::initServerEndpoints() {
	execOperations.makeWellKnownEndpoint(WLTOKEN_CLIENTPROXY_EXECOPERATIONS, TaskPriority::DefaultEndpoint);
}

namespace ClientProxy {

struct ProxyRequestState;
using ProxyRequestReference = Reference<ProxyRequestState>;

struct ProxyTransaction : public ReferenceCounted<ProxyTransaction> {
	Reference<ReadYourWritesTransaction> tx;
	uint32_t lastExecSeqNo = 0;
	std::unordered_map<uint32_t, ProxyRequestReference> pendingRequests;
	std::unordered_map<uint32_t, ProxyRequestReference> activeRequests;
};

using ProxyTransactionReference = Reference<ProxyTransaction>;

struct ProxyState {
	Database db;
	using TransactionMap = std::unordered_map<UID, ProxyTransactionReference>;
	TransactionMap transactionMap;

	ProxyState(Reference<IClusterConnectionRecord> connRecord, LocalityData clientLocality) {
		db = Database::createDatabase(connRecord, Database::API_VERSION_LATEST, IsInternal::False, clientLocality);
	}

	ProxyTransactionReference getTransaction(UID transactionID) {
		auto iter = transactionMap.find(transactionID);
		if (iter != transactionMap.end()) {
			return iter->second;
		}
		ProxyTransactionReference tx = makeReference<ProxyTransaction>();
		transactionMap[transactionID] = tx;
		return tx;
	}

	void releaseTransaction(UID transactionID) {
		auto iter = transactionMap.find(transactionID);
		if (iter != transactionMap.end()) {
			transactionMap.erase(iter);
		}
	}
};

struct ProxyRequestState : public ReferenceCounted<ProxyRequestState> {
	enum class State { PENDING, STARTED, COMPLETED };

	State st = State::PENDING;
	ExecOperationsReference request;
	ProxyTransactionReference proxyTransaction;
	std::vector<Future<Void>> execActors;

	Reference<ReadYourWritesTransaction> transaction() { return proxyTransaction->tx; }

	void actorStarted(Future<Void> actor) {
		st = State::STARTED;
		execActors.push_back(actor);
		proxyTransaction->activeRequests[request->firstSeqNo] = ProxyRequestReference::addRef(this);
	}

	void completed() {
		st = State::COMPLETED;
		auto iter = proxyTransaction->activeRequests.find(request->firstSeqNo);
		if (iter != proxyTransaction->activeRequests.end()) {
			proxyTransaction->activeRequests.erase(iter);
		}
	}
};

ACTOR template <class ResultType, class T>
Future<Void> executeAndReplyActor(ProxyRequestReference req, Future<T> f) {
	try {
		T result = wait(f);
		req->request->reply.send(ExecOperationsReply{ OperationResult(ResultType{ { result } }) });
	} catch (Error& e) {
		req->request->reply.sendError(e);
	}
	req->completed();
	return Void();
}

template <class ResultType, class T>
void replyAfterCompletion(ProxyRequestReference req, Future<T> f) {
	req->actorStarted(executeAndReplyActor<ResultType, T>(req, f));
}

void executeGetOp(const GetOp& op, ProxyRequestReference req) {
	auto future = req->transaction()->get(op.key, Snapshot{ op.snapshot });
	replyAfterCompletion<GetResult>(req, future);
}

void executeGetRangeOp(const GetRangeOp& op, ProxyRequestReference req) {
	Future<RangeResult> future =
	    req->transaction()->getRange(op.begin, op.end, op.limits, Snapshot{ op.snapshot }, Reverse{ op.reverse });
	replyAfterCompletion<GetRangeResult>(req, future);
}

void executeSetOp(const SetOp& op, ProxyRequestReference req) {
	req->transaction()->set(op.key, op.value);
}

ACTOR Future<Void> executeCommitActor(ProxyRequestReference req) {
	try {
		wait(req->transaction()->commit());
		req->request->reply.send(
		    ExecOperationsReply{ OperationResult(Int64Result{ { req->transaction()->getCommittedVersion() } }) });
	} catch (Error& e) {
		req->request->reply.sendError(e);
	}
	req->completed();
	return Void();
}

void executeCommitOp(const CommitOp& op, ProxyRequestReference req) {
	req->actorStarted(executeCommitActor(req));
}

void executeSetOptionOp(const SetOptionOp& op, ProxyRequestReference req) {
	req->transaction()->setOption((FDBTransactionOptions::Option)op.optionId, op.value);
}

void executeResetOp(const ResetOp& op, ProxyRequestReference req) {
	req->transaction()->reset();
}

void executeClearOp(const ClearOp& op, ProxyRequestReference req) {
	req->transaction()->clear(op.key);
}

void executeClearRangeOp(const ClearRangeOp& op, ProxyRequestReference req) {
	if (op.begin > op.end)
		throw inverted_range();

	req->transaction()->clear(KeyRangeRef(op.begin, op.end));
}

void executeGetReadVersionOp(const GetReadVersionOp& op, ProxyRequestReference req) {
	auto future = req->transaction()->getReadVersion();
	replyAfterCompletion<Int64Result>(req, future);
}

void executeAddReadConflictRangeOp(const AddReadConflictRangeOp& op, ProxyRequestReference req) {
	req->transaction()->addReadConflictRange(op.range);
}

void executeOnErrorOp(const OnErrorOp& op, ProxyRequestReference req) {
	auto future = req->transaction()->onError(Error::fromUnvalidatedCode(op.errorCode));
	replyAfterCompletion<VoidResult>(req, future);
}

void executeOperations(ProxyState* rpcProxyData, ProxyRequestReference req) {

	try {
		if (req->proxyTransaction->lastExecSeqNo == 0) {
			req->proxyTransaction->tx = makeReference<ReadYourWritesTransaction>(rpcProxyData->db);
		}

		for (auto op : req->request->operations) {
			// only the last operation can start an actor
			if (req->st != ProxyRequestState::State::PENDING) {
				throw client_invalid_operation();
			}

			switch (op.index()) {
			case OP_GET:
				executeGetOp(std::get<OP_GET>(op), req);
				break;
			case OP_GETRANGE:
				executeGetRangeOp(std::get<OP_GETRANGE>(op), req);
				break;
			case OP_SET:
				executeSetOp(std::get<OP_SET>(op), req);
				break;
			case OP_COMMIT:
				executeCommitOp(std::get<OP_COMMIT>(op), req);
				break;
			case OP_SETOPTION:
				executeSetOptionOp(std::get<OP_SETOPTION>(op), req);
				break;
			case OP_RESET:
				executeResetOp(std::get<OP_RESET>(op), req);
				break;
			case OP_CLEAR:
				executeClearOp(std::get<OP_CLEAR>(op), req);
				break;
			case OP_CLEARRANGE:
				executeClearRangeOp(std::get<OP_CLEARRANGE>(op), req);
				break;
			case OP_GETREADVERSION:
				executeGetReadVersionOp(std::get<OP_GETREADVERSION>(op), req);
				break;
			case OP_ADDREADCONFLICTRANGE:
				executeAddReadConflictRangeOp(std::get<OP_ADDREADCONFLICTRANGE>(op), req);
				break;
			case OP_ONERROR:
				executeOnErrorOp(std::get<OP_ONERROR>(op), req);
				break;
			}
		}

		// check if the last operation sent a reply
		if (req->st == ProxyRequestState::State::PENDING) {
			throw client_invalid_operation();
		}
	} catch (Error& e) {
		req->request->reply.sendError(e);
	}
	req->proxyTransaction->lastExecSeqNo += req->request->operations.size();
}

ProxyState* createProxyState(Reference<IClusterConnectionRecord> connRecord, LocalityData clientLocality) {
	return new ProxyState(connRecord, clientLocality);
}

void destroyProxyState(ProxyState* proxyState) {
	delete proxyState;
}

void releaseTransaction(ProxyState* proxyState, UID transactionID) {
	proxyState->releaseTransaction(transactionID);
}

void handleExecOperationsRequest(ProxyState* rpcProxyData, ExecOperationsReference request) {
	for (uint64_t txID : request->releasedTransactions) {
		rpcProxyData->releaseTransaction(UID(request->clientID, txID));
	}

	UID transactionID(request->clientID, request->transactionID);
	Reference<ProxyTransaction> proxyTx = rpcProxyData->getTransaction(transactionID);
	ProxyRequestReference proxyRequest = makeReference<ProxyRequestState>();
	proxyRequest->request = request;
	proxyRequest->proxyTransaction = proxyTx;

	// If previous operations not yet executed, enqueue the current request as pending
	if (proxyTx->lastExecSeqNo != request->firstSeqNo) {
		proxyTx->pendingRequests[request->firstSeqNo] = proxyRequest;
		return;
	}

	// Execute the current request
	executeOperations(rpcProxyData, proxyRequest);

	// Execute pending requests
	while (true) {
		auto reqIter = proxyTx->pendingRequests.find(proxyTx->lastExecSeqNo);
		if (reqIter == proxyTx->pendingRequests.end()) {
			break;
		}
		executeOperations(rpcProxyData, reqIter->second);
		proxyTx->pendingRequests.erase(reqIter);
	}
}

ACTOR Future<Void> proxyServer(ClientProxyInterface interface,
                               Reference<IClusterConnectionRecord> connRecord,
                               LocalityData clientLocality) {
	state ProxyState proxyData(connRecord, clientLocality);
	loop {
		ExecOperationsRequest req = waitNext(interface.execOperations.getFuture());
		ExecOperationsReference reqRef = makeReference<ExecOperationsRequestRefCounted>(req);
		handleExecOperationsRequest(&proxyData, reqRef);
	}
}

} // namespace ClientProxy
