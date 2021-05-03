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
	registerActiveRecord(r)
	registerAllocator(r)
	registerAlterPK(r)
	registerAutoUpgrade(r)
	registerBackup(r)
	registerBackupNodeShutdown(r)
	registerCancel(r)
	registerCDC(r)
	registerClearRange(r)
	registerClockJumpTests(r)
	registerClockMonotonicTests(r)
	registerConnectionLatencyTest(r)
	registerCopy(r)
	registerDecommission(r)
	registerDiskFull(r)
	registerDiskStalledDetection(r)
	registerDjango(r)
	registerDrop(r)
	registerElectionAfterRestart(r)
	registerEncryption(r)
	registerEngineSwitch(r)
	registerFlowable(r)
	registerFollowerReads(r)
	registerGopg(r)
	registerGossip(r)
	registerGORM(r)
	registerHibernate(r, hibernateOpts)
	registerHibernate(r, hibernateSpatialOpts)
	registerHotSpotSplits(r)
	registerImportDecommissioned(r)
	registerImportMixedVersion(r)
	registerImportTPCC(r)
	registerImportTPCH(r)
	registerImportNodeShutdown(r)
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
	registerLiquibase(r)
	registerNamespaceUpgradeMigration(r)
	registerNetwork(r)
	registerPebble(r)
	registerPgjdbc(r)
	registerPgx(r)
	registerNodeJSPostgres(r)
	registerPsycopg(r)
	registerQueue(r)
	registerQuitAllNodes(r)
	registerQuitTransfersLeases(r)
	registerRebalanceLoad(r)
	registerReplicaGC(r)
	registerRestart(r)
	registerRestoreNodeShutdown(r)
	registerRestore(r)
	registerRoachmart(r)
	registerSchemaChangeBulkIngest(r)
	registerSchemaChangeDatabaseVersionUpgrade(r)
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
	registerSequelize(r)
	registerSequenceUpgrade(r)
	registerSQLAlchemy(r)
	registerSQLSmith(r)
	registerSyncTest(r)
	registerSysbench(r)
	registerTPCC(r)
	registerTPCDSVec(r)
	registerTPCE(r)
	registerTPCHVec(r)
	registerKVBench(r)
	registerTypeORM(r)
	registerLoadSplits(r)
	registerVersion(r)
	registerYCSB(r)
	registerTPCHBench(r)
	registerOverload(r)
	registerMultiTenantUpgrade(r)
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
