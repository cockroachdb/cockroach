// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

func registerTests(r *testRegistry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*testRegistry\) {' pkg/cmd/roachtest/*.go | grep -E -o 'register[^(]+' | grep -E -v '^register(Tests|Benchmarks)$' | grep -v '^\w*Bench$' | sort -f | awk '{printf "\t%s(r)\n", $0}'

	registerAcceptance(r)
	registerAllocator(r)
	registerAlterPK(r)
	registerAutoUpgrade(r)
	registerBackup(r)
	registerCancel(r)
	registerCDC(r)
	registerClearRange(r)
	registerClockJumpTests(r)
	registerClockMonotonicTests(r)
	registerCopy(r)
	registerDecommission(r)
	registerDiskFull(r)
	registerDiskStalledDetection(r)
	registerDjango(r)
	registerDrop(r)
	registerDumpBackwardsCompat(r)
	registerElectionAfterRestart(r)
	registerEncryption(r)
	registerEngineSwitch(r)
	registerFlowable(r)
	registerFollowerReads(r)
	registerGopg(r)
	registerGossip(r)
	registerHibernate(r)
	registerHotSpotSplits(r)
	registerImportTPCC(r)
	registerImportTPCH(r)
	registerInconsistency(r)
	registerIndexes(r)
	registerInterleaved(r)
	registerJepsen(r)
	registerJobsMixedVersions(r)
	registerKV(r)
	registerKVContention(r)
	registerKVQuiescenceDead(r)
	registerKVGracefulDraining(r)
	registerKVScalability(r)
	registerKVSplits(r)
	registerKVRangeLookups(r)
	registerLargeRange(r)
	registerLedger(r)
	registerLibPQ(r)
	registerNamespaceUpgrade(r)
	registerNetwork(r)
	registerPgjdbc(r)
	registerPgx(r)
	registerPsycopg(r)
	registerQueue(r)
	registerQuitAllNodes(r)
	registerQuitTransfersLeases(r)
	registerRebalanceLoad(r)
	registerReplicaGC(r)
	registerRestart(r)
	registerRestore(r)
	registerRoachmart(r)
	registerScaleData(r)
	registerSchemaChangeBulkIngest(r)
	registerSchemaChangeDuringKV(r)
	registerSchemaChangeIndexTPCC100(r)
	registerSchemaChangeIndexTPCC1000(r)
	registerSchemaChangeDuringTPCC1000(r)
	registerSchemaChangeInvertedIndex(r)
	registerSchemaChangeMixedVersions(r)
	registerSchemaChangeRandomLoad(r)
	registerScrubAllChecksTPCC(r)
	registerScrubIndexOnlyTPCC(r)
	registerSecondaryIndexesMultiVersionCluster(r)
	registerSQLAlchemy(r)
	registerSQLSmith(r)
	registerSyncTest(r)
	registerSysbench(r)
	registerTPCC(r)
	registerTPCDSVec(r)
	registerTPCHVec(r)
	registerKVBench(r)
	registerTypeORM(r)
	registerLoadSplits(r)
	registerVersion(r)
	registerYCSB(r)
	registerTPCHBench(r)
	registerOverload(r)
}

func registerBenchmarks(r *testRegistry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*registry\) {' *.go | grep -E -o 'register[^(]+' | grep -v '^registerTests$' | grep '^\w*Bench$' | sort | awk '{printf "\t%s(r)\n", $0}'

	registerIndexesBench(r)
	registerTPCCBench(r)
	registerKVBench(r)
	registerTPCHBench(r)
}
