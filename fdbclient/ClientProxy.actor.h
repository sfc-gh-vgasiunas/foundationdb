/*
 * ClientProxy.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_CLIENTPROXY_ACTOR_G_H)
#define FDBCLIENT_CLIENTPROXY_ACTOR_G_H
#include "fdbclient/ClientProxy.actor.g.h"
#elif !defined(FDBCLIENT_CLIENTPROXY_ACTOR_H)
#define FDBCLIENT_CLIENTPROXY_ACTOR_H

#include "flow/flow.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ClientProxyInterface.h"
#include "flow/actorcompiler.h"

namespace ClientProxy {

ACTOR Future<Void> proxyServer(ClientProxyInterface interface,
                               Reference<IClusterConnectionRecord> connRecord,
                               LocalityData clientLocality);

}

#endif