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

struct ProxyTransaction : public ReferenceCounted<ProxyTransaction> {
	Reference<ReadYourWritesTransaction> tx;
	uint32_t lastExecSeqNo = 0;
	std::unordered_map<uint32_t, ExecOperationsRequest> pendingRequests;
	ReplyPromise<ExecOperationsReply> currentReply;
	std::vector<Future<Void>> execActors;
};

struct ProxyState {
	Database db;
	using TransactionMap = std::unordered_map<UID, Reference<ProxyTransaction>>;
	TransactionMap transactionMap;

	ProxyState(Reference<IClusterConnectionRecord> connRecord, LocalityData clientLocality) {
		db = Database::createDatabase(connRecord, Database::API_VERSION_LATEST, IsInternal::False, clientLocality);
	}

	Reference<ProxyTransaction> getTransaction(UID transactionID) {
		auto iter = transactionMap.find(transactionID);
		if (iter != transactionMap.end()) {
			return iter->second;
		}
		Reference<ProxyTransaction> tx = makeReference<ProxyTransaction>();
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

ACTOR template <class ResultType, class T>
Future<Void> executeAndReplyActor(ReplyPromise<ExecOperationsReply> reply, Future<T> f) {
	try {
		T result = wait(f);
		reply.send(ExecOperationsReply{ OperationResult(ResultType{ { result } }) });
	} catch (Error& e) {
		reply.sendError(e);
	}
	return Void();
}

template <class ResultType, class T>
void replyAfterCompletion(Reference<ProxyTransaction> proxyTx, Future<T> f) {
	proxyTx->execActors.push_back(executeAndReplyActor<ResultType, T>(proxyTx->currentReply, f));
	proxyTx->currentReply.reset();
}

void executeGetOp(const GetOp& op, Reference<ProxyTransaction> proxyTx) {
	auto future = proxyTx->tx->get(op.key, Snapshot{ op.snapshot });
	replyAfterCompletion<GetResult>(proxyTx, future);
}

void executeGetRangeOp(const GetRangeOp& op, Reference<ProxyTransaction> proxyTx) {
	Future<RangeResult> future =
	    proxyTx->tx->getRange(op.begin, op.end, op.limits, Snapshot{ op.snapshot }, Reverse{ op.reverse });
	replyAfterCompletion<GetRangeResult>(proxyTx, future);
}

void executeSetOp(const SetOp& op, Reference<ProxyTransaction> proxyTx) {
	proxyTx->tx->set(op.key, op.value);
}

ACTOR Future<Void> executeCommitActor(ReplyPromise<ExecOperationsReply> reply, Reference<ProxyTransaction> proxyTx) {
	try {
		wait(proxyTx->tx->commit());
		reply.send(ExecOperationsReply{ OperationResult(Int64Result{ { proxyTx->tx->getCommittedVersion() } }) });
	} catch (Error& e) {
		reply.sendError(e);
	}
	return Void();
}

void executeCommitOp(const CommitOp& op, Reference<ProxyTransaction> proxyTx) {
	proxyTx->execActors.push_back(executeCommitActor(proxyTx->currentReply, proxyTx));
	proxyTx->currentReply.reset();
}

void executeSetOptionOp(const SetOptionOp& op, Reference<ProxyTransaction> proxyTx) {
	proxyTx->tx->setOption((FDBTransactionOptions::Option)op.optionId, op.value);
}

void executeResetOp(const ResetOp& op, Reference<ProxyTransaction> proxyTx) {
	proxyTx->tx->reset();
}

void executeClearOp(const ClearOp& op, Reference<ProxyTransaction> proxyTx) {
	proxyTx->tx->clear(op.key);
}

void executeClearRangeOp(const ClearRangeOp& op, Reference<ProxyTransaction> proxyTx) {
	if (op.begin > op.end)
		throw inverted_range();

	proxyTx->tx->clear(KeyRangeRef(op.begin, op.end));
}

void executeGetReadVersionOp(const GetReadVersionOp& op, Reference<ProxyTransaction> proxyTx) {
	auto future = proxyTx->tx->getReadVersion();
	replyAfterCompletion<Int64Result>(proxyTx, future);
}

void executeOperations(ProxyState* rpcProxyData,
                       Reference<ProxyTransaction> proxyTx,
                       const ExecOperationsRequest& request) {

	try {
		if (proxyTx->lastExecSeqNo == 0) {
			proxyTx->tx = makeReference<ReadYourWritesTransaction>(rpcProxyData->db);
		}

		proxyTx->currentReply = request.reply;
		for (auto op : request.operations) {
			// the operatin sending a reply must be the last in the sequence
			if (!proxyTx->currentReply.isValid()) {
				throw client_invalid_operation();
			}
			switch (op.index()) {
			case OP_GET:
				executeGetOp(std::get<OP_GET>(op), proxyTx);
				break;
			case OP_GETRANGE:
				executeGetRangeOp(std::get<OP_GETRANGE>(op), proxyTx);
				break;
			case OP_SET:
				executeSetOp(std::get<OP_SET>(op), proxyTx);
				break;
			case OP_COMMIT:
				executeCommitOp(std::get<OP_COMMIT>(op), proxyTx);
				break;
			case OP_SETOPTION:
				executeSetOptionOp(std::get<OP_SETOPTION>(op), proxyTx);
				break;
			case OP_RESET:
				executeResetOp(std::get<OP_RESET>(op), proxyTx);
				break;
			case OP_CLEAR:
				executeClearOp(std::get<OP_CLEAR>(op), proxyTx);
				break;
			case OP_CLEARRANGE:
				executeClearRangeOp(std::get<OP_CLEARRANGE>(op), proxyTx);
				break;
			case OP_GETREADVERSION:
				executeGetReadVersionOp(std::get<OP_GETREADVERSION>(op), proxyTx);
				break;
			}
		}
	} catch (Error& e) {
		request.reply.sendError(e);
	}
	proxyTx->lastExecSeqNo += request.operations.size();
}

void handleExecOperationsRequest(ProxyState* rpcProxyData, const ExecOperationsRequest& request) {
	for (uint64_t txID : request.releasedTransactions) {
		rpcProxyData->releaseTransaction(UID(request.clientID, txID));
	}

	UID transactionID(request.clientID, request.transactionID);
	Reference<ProxyTransaction> proxyTx = rpcProxyData->getTransaction(transactionID);

	// If previous operations not yet executed, enqueue the current request as pending
	if (proxyTx->lastExecSeqNo != request.firstSeqNo) {
		proxyTx->pendingRequests[request.firstSeqNo] = request;
		return;
	}

	// Execute the current request
	executeOperations(rpcProxyData, proxyTx, request);

	// Execute pending requests
	while (true) {
		auto reqIter = proxyTx->pendingRequests.find(proxyTx->lastExecSeqNo);
		if (reqIter == proxyTx->pendingRequests.end()) {
			break;
		}
		executeOperations(rpcProxyData, proxyTx, reqIter->second);
		proxyTx->pendingRequests.erase(reqIter);
	}
}

ACTOR Future<Void> proxyServer(ClientProxyInterface interface,
                               Reference<IClusterConnectionRecord> connRecord,
                               LocalityData clientLocality) {
	state Database localDb;
	state ProxyState proxyData(connRecord, clientLocality);
	loop {
		ExecOperationsRequest req = waitNext(interface.execOperations.getFuture());
		handleExecOperationsRequest(&proxyData, req);
	}
}

} // namespace ClientProxy
