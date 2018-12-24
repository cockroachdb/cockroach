// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

func registerTests(r *registry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*registry\) {' *.go | grep -E -o 'register[^(]+' | grep -E -v '^register(Tests|Benchmarks)$' | grep -v '^\w*Bench$' | sort -f | awk '{printf "\t%s(r)\n", $0}'

	registerAcceptance(r)
	registerAllocator(r)
	registerBackup(r)
	registerCancel(r)
	registerCDC(r)
	registerClearRange(r)
	registerClock(r)
	registerCopy(r)
	registerDebug(r)
	registerDebugHeap(r)
	registerDecommission(r)
	registerDiskUsage(r)
	registerDiskFull(r)
	registerDrop(r)
	registerElectionAfterRestart(r)
	registerEncryption(r)
	registerGossip(r)
	registerHibernate(r)
	registerHotSpotSplits(r)
	registerImportTPCC(r)
	registerImportTPCH(r)
	registerInterleaved(r)
	registerJepsen(r)
	registerKV(r)
	registerKVQuiescenceDead(r)
	registerKVGracefulDraining(r)
	registerKVScalability(r)
	registerKVSplits(r)
	registerLargeRange(r)
	registerQueue(r)
	registerRebalanceLoad(r)
	registerReplicaGC(r)
	registerRestore(r)
	registerRoachmart(r)
	registerScaleData(r)
	registerSchemaChangeKV(r)
	registerSchemaChangeIndexTPCC100(r)
	registerSchemaChangeIndexTPCC1000(r)
	registerScrubAllChecksTPCC(r)
	registerScrubIndexOnlyTPCC(r)
	registerSchemaChangeCancelIndexTPCC1000(r)
	registerSQLsmith(r)
	registerSyncTest(r)
	registerSysbench(r)
	registerTPCC(r)
	registerLoadSplits(r)
	registerUpgrade(r)
	registerVersion(r)
	registerYCSB(r)
}

func registerBenchmarks(r *registry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*registry\) {' *.go | grep -E -o 'register[^(]+' | grep -v '^registerTests$' | grep '^\w*Bench$' | sort | awk '{printf "\t%s(r)\n", $0}'

	registerTPCCBench(r)
}
