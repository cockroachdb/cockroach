// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDropIndexWithZoneConfigCCL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numRows = 100

	params, _ := tests.CreateTestServerParams()
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(context.Background())

	// Create a test table with a partitioned secondary index.
	if err := tests.CreateKVTable(sqlDB.DB, numRows); err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `CREATE INDEX i ON t.kv (v) PARTITION BY LIST (v) (
		PARTITION p1 VALUES IN (1),
		PARTITION p2 VALUES IN (2)
	)`)
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	indexDesc, _, err := tableDesc.FindIndexByName("i")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(indexDesc.ID)
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

	// Set zone configs on the primary index, secondary index, and one partition
	// of the secondary index.
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX t.kv@primary", "")
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX t.kv@i", "")
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p2 OF TABLE t.kv", "")
	for _, target := range []string{"t.kv@primary", "t.kv@i", "t.kv.p2"} {
		if exists := sqlutils.ZoneConfigExists(t, sqlDB, target); !exists {
			t.Fatalf(`zone config for %s does not exist`, target)
		}
	}

	// Drop the index and verify that the zone config for the secondary index and
	// its partition are removed but the zone config for the primary index
	// remains.
	sqlDB.Exec(t, `DROP INDEX t.kv@i`)
	tests.CheckKeyCount(t, kvDB, indexSpan, 0)
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	if _, _, err := tableDesc.FindIndexByName("i"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
	if exists := sqlutils.ZoneConfigExists(t, sqlDB, "t.kv@primary"); !exists {
		t.Fatal("zone config for primary index removed after dropping secondary index")
	}
	for _, target := range []string{"t.kv@i", "t.kv.p2"} {
		if exists := sqlutils.ZoneConfigExists(t, sqlDB, target); exists {
			t.Fatalf(`zone config for %s still exists`, target)
		}
	}
}
