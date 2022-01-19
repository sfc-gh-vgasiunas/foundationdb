/*
 * ClientRPCInterface.h
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

#ifndef FDBCLIENT_CLIENTPRPCINTERFACE_H
#define FDBCLIENT_CLIENTPRPCINTERFACE_H
#pragma once

#include "fdbclient/ClientProxyInterface.h"
#include "flow/ThreadHelper.actor.h"

using ExecOperationsReplySAV = ThreadSingleAssignmentVar<ClientProxy::ExecOperationsReply>;

class ClientRPCInterface : public ThreadSafeReferenceCounted<ClientRPCInterface> {
public:
	virtual ~ClientRPCInterface() {}
	virtual void executeOperations(ClientProxy::ExecOperationsReference request, ExecOperationsReplySAV* result) = 0;
	virtual void releaseTransaction(uint64_t transaction) = 0;
	virtual uint64_t getClientID() = 0;
};

#endif