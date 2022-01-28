/*
 * LocalClientAPI.cpp
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

#include "fdbclient/LocalClientAPI.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/ClientProxyStub.h"

IClientApi* getLocalClientAPI() {
	static IClientApi* api = new ThreadSafeApi();
	return api;
}

IClientApi* getClientProxyAPI() {
	static IClientApi* api = new ClientProxyAPIStub();
	return api;
}

Reference<IDatabase> createDLProxyDatabase(const char* clusterFile, Reference<FDBProxyCApi> proxyApi) {
	return ((ClientProxyAPIStub*)getClientProxyAPI())->createDLProxyDatabase(clusterFile, proxyApi);
}