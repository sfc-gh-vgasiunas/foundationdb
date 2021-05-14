/*
 * ConfigBroadcaster.actor.cpp
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

#include "fdbclient/CoordinationInterface.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/LocalConfiguration.h"
#include "fdbclient/Tuple.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

std::map<Key, Value> const testManualKnobOverrides = {
	{ "test_double"_sr, "1.0"_sr },
};

TestKnobs const& getExpectedTestKnobsEmptyConfig() {
	static std::unique_ptr<TestKnobs> knobs;
	if (!knobs) {
		knobs = std::make_unique<TestKnobs>();
		knobs->setKnob("test_double", "1.0");
	}
	return *knobs;
}

TestKnobs const& getExpectedTestKnobsFinal() {
	static std::unique_ptr<TestKnobs> knobs;
	if (!knobs) {
		knobs = std::make_unique<TestKnobs>();
		knobs->setKnob("test_long", "100");
		knobs->setKnob("test_int", "2");
		knobs->setKnob("test_bool", "true");
		knobs->setKnob("test_string", "x");
		knobs->setKnob("test_double", "1.0");
	}
	return *knobs;
}

ACTOR template <class ConfigStore>
Future<Void> setTestSnapshot(ConfigStore* configStore, Version* lastWrittenVersion) {
	TraceEvent("WritingTestSnapshot").detail("LastWrittenVersion", *lastWrittenVersion);
	std::map<ConfigKey, Value> snapshot = {
		{ ConfigKeyRef("class-A"_sr, "test_int"_sr), "1"_sr },
		{ ConfigKeyRef("class-B"_sr, "test_int"_sr), "2"_sr },
		{ ConfigKeyRef("class-C"_sr, "test_int"_sr), "3"_sr },
		{ ConfigKeyRef("class-B"_sr, "test_double"_sr), "4.0"_sr },
		{ ConfigKeyRef("class-A"_sr, "test_string"_sr), "x"_sr },
	};
	wait(configStore->setSnapshot(std::move(snapshot), ++(*lastWrittenVersion)));
	return Void();
}

void appendVersionedMutation(Standalone<VectorRef<VersionedConfigMutationRef>>& versionedMutations,
                             Version version,
                             KeyRef configClass,
                             KeyRef knobName,
                             ValueRef knobValue) {
	Tuple tuple;
	tuple << configClass;
	tuple << knobName;
	auto mutation = ConfigMutationRef::createConfigMutation(tuple.pack(), knobValue);
	versionedMutations.emplace_back_deep(versionedMutations.arena(), version, mutation);
}

ACTOR template <class ConfigStore>
Future<Void> addTestUpdates(ConfigStore* configStore, Version* lastWrittenVersion) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	++(*lastWrittenVersion);
	appendVersionedMutation(versionedMutations, *lastWrittenVersion, "class-A"_sr, "test_bool"_sr, "true"_sr);
	appendVersionedMutation(versionedMutations, *lastWrittenVersion, "class-B"_sr, "test_long"_sr, "100"_sr);
	appendVersionedMutation(versionedMutations, *lastWrittenVersion, "class-C"_sr, "test_double"_sr, "10.0"_sr);
	appendVersionedMutation(versionedMutations, *lastWrittenVersion, "class-A"_sr, "test_int"_sr, "10"_sr);
	wait(configStore->addVersionedMutations(versionedMutations, *lastWrittenVersion));
	return Void();
}

Value versionToValue(Version version) {
	auto s = format("%ld", version);
	return StringRef(reinterpret_cast<uint8_t const*>(s.c_str()), s.size());
}

template <class ConfigStore>
Future<Void> addSequentialTestUpdates(ConfigStore &configStore, Version &lastWrittenVersion) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	++lastWrittenVersion;
	appendVersionedMutation(versionedMutations, lastWrittenVersion, "class-A"_sr, "test_long"_sr, versionToValue(lastWrittenVersion));
	appendVersionedMutation(versionedMutations, lastWrittenVersion, "class-B"_sr, "test_long"_sr, versionToValue(lastWrittenVersion * 10));
	return configStore.addVersionedMutations(versionedMutations, lastWrittenVersion);
}

ACTOR template <class ConfigStore>
Future<Void> runTestUpdates(ConfigStore* configStore, Version* lastWrittenVersion) {
	wait(setTestSnapshot(configStore, lastWrittenVersion));
	wait(addTestUpdates(configStore, lastWrittenVersion));
	// TODO: Clean up on-disk state
	return Void();
}

ACTOR Future<Void> runFirstLocalConfiguration(std::string configPath, UID id) {
	state LocalConfiguration localConfiguration(configPath, testManualKnobOverrides);
	state Version lastWrittenVersion = 0;
	wait(localConfiguration.initialize("./", id));
	wait(runTestUpdates(&localConfiguration, &lastWrittenVersion));
	{
		auto const *conf = &localConfiguration;
		wait(waitUntil([conf] {
			return conf->getTestKnobs() == getExpectedTestKnobsFinal();
		}));
	}
	return Void();
}

ACTOR Future<Void> runSecondLocalConfiguration(std::string configPath, UID id, TestKnobs const* expectedTestKnobs) {
	state LocalConfiguration localConfiguration(configPath, testManualKnobOverrides);
	wait(localConfiguration.initialize("./", id));
	ASSERT(localConfiguration.getTestKnobs() == *expectedTestKnobs);
	{
		auto const *conf = &localConfiguration;
		auto const *expected = expectedTestKnobs;
		wait(waitUntil([conf, expected] {
			return conf->getTestKnobs() == *expected;
		}));
	}
	return Void();
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Simple") {
	wait(runFirstLocalConfiguration("class-A/class-B", deterministicRandom()->randomUniqueID()));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Restart") {
	state UID id = deterministicRandom()->randomUniqueID();
	wait(runFirstLocalConfiguration("class-A/class-B", id));
	wait(runSecondLocalConfiguration("class-A/class-B", id, &getExpectedTestKnobsFinal()));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/FreshRestart") {
	state UID id = deterministicRandom()->randomUniqueID();
	wait(runFirstLocalConfiguration("class-A/class-B", id));
	wait(runSecondLocalConfiguration("class-B/class-A", id, &getExpectedTestKnobsEmptyConfig()));
	return Void();
}

namespace {

class DummyConfigSource {
	ConfigFollowerInterface cfi;
	ACTOR static Future<Void> serve(DummyConfigSource* self) {
		loop {
			choose {
				when(ConfigFollowerGetVersionRequest req = waitNext(self->cfi.getVersion.getFuture())) {
					req.reply.send(0);
				}
				when(ConfigFollowerGetSnapshotRequest req = waitNext(self->cfi.getSnapshot.getFuture())) {
					req.reply.send(ConfigFollowerGetSnapshotReply{});
				}
			}
		}
	}

public:
	Future<Void> serve() { return serve(this); }
	ConfigFollowerInterface const& getInterface() { return cfi; }
};

ACTOR template<class F>
Future<Void> waitUntil(F isReady) {
	loop {
		if (isReady()) { return Void(); }
		wait(delayJittered(0.1));
	}
}

Future<Void> waitUntilConfigsMatch(LocalConfiguration const &fst, LocalConfiguration const &snd) {
	return waitUntil([&fst, &snd] {
		return fst.getTestKnobs() == snd.getTestKnobs();
	});
}

Future<Void> waitUntilTestLongMatches(LocalConfiguration const &conf, int64_t expectedValue) {
	return waitUntil([&conf, expectedValue]{
		return conf.getTestKnobs().TEST_LONG == expectedValue;
	});
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/ConfigBroadcaster/CheckpointedUpdates") {
	state DummyConfigSource dummyConfigSource;
	state ConfigBroadcaster broadcaster1(dummyConfigSource.getInterface(), deterministicRandom()->randomUniqueID());
	state ConfigBroadcaster broadcaster2(dummyConfigSource.getInterface(), deterministicRandom()->randomUniqueID());
	state Reference<AsyncVar<ConfigFollowerInterface>> cfi = makeReference<AsyncVar<ConfigFollowerInterface>>();
	state LocalConfiguration localConfigurationA("class-A", testManualKnobOverrides);
	state LocalConfiguration localConfigurationB("class-B", testManualKnobOverrides);
	state Version version = 0;
	state ActorCollection actors(false);
	state Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	wait(localConfigurationA.initialize("./", deterministicRandom()->randomUniqueID()));
	wait(localConfigurationB.initialize("./", deterministicRandom()->randomUniqueID()));
	TraceEvent("StartedTestBroadcasterAndLocalConfigs")
	    .detail("Broadcaster1", broadcaster1.getID())
		.detail("Broadcaster2", broadcaster2.getID())
	    .detail("LocalConfigurationA", localConfigurationA.getID())
	    .detail("LocalConfigurationB", localConfigurationB.getID());
	actors.add(dummyConfigSource.serve());
	actors.add(broadcaster1.serve(cfi->get()));
	actors.add(localConfigurationA.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(cfi)));
	actors.add(localConfigurationB.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(cfi)));
	while (version < 10) {
		wait(addSequentialTestUpdates(broadcaster1, version));
		versionedMutations = Standalone<VectorRef<VersionedConfigMutationRef>>{};
		wait(waitUntilTestLongMatches(localConfigurationA, version));
		wait(waitUntilTestLongMatches(localConfigurationB, version * 10));
	}

	// Test changing broadcaster
	cfi->set(ConfigFollowerInterface{});
	actors.add(broadcaster2.serve(cfi->get()));
	while (version < 20) {
		wait(addSequentialTestUpdates(broadcaster2, version));
		wait(waitUntilTestLongMatches(localConfigurationA, version));
		wait(waitUntilTestLongMatches(localConfigurationB, version * 10));
	}

	// Test compaction
	while (version < 30) {
		wait(addSequentialTestUpdates(broadcaster2, version));
	}
	wait(cfi->get().compact.getReply(ConfigFollowerCompactRequest{ version }));
	wait(waitUntilTestLongMatches(localConfigurationA, 30));
	wait(waitUntilTestLongMatches(localConfigurationB, 300));
	return Void();
}
