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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func subzoneExists(cfg *zonepb.ZoneConfig, index uint32, partition string) bool {
	for _, s := range cfg.Subzones {
		if s.IndexID == index && s.PartitionName == partition {
			return true
		}
	}
	return false
}

func TestDropIndexWithZoneConfigCCL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numRows = 100

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	asyncNotification := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		GCJob: &sql.GCJobTestingKnobs{
			RunBeforeResume: func(_ int64) error {
				<-asyncNotification
				return nil
			},
		},
	}
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(context.Background())

	// Create a test table with a partitioned secondary index.
	if err := tests.CreateKVTable(sqlDBRaw, "kv", numRows); err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `CREATE INDEX i ON t.kv (v) PARTITION BY LIST (v) (
		PARTITION p1 VALUES IN (1),
		PARTITION p2 VALUES IN (2)
	)`)
	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	indexDesc, _, err := tableDesc.FindIndexByName("i")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, indexDesc.ID)
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

	// Set zone configs on the primary index, secondary index, and one partition
	// of the secondary index.
	ttlYaml := "gc: {ttlseconds: 1}"
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX t.kv@primary", "")
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX t.kv@i", ttlYaml)
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p2 OF INDEX t.kv@i", ttlYaml)

	// Drop the index and verify that the zone config for the secondary index and
	// its partition are removed but the zone config for the primary index
	// remains.
	sqlDB.Exec(t, `DROP INDEX t.kv@i`)
	// All zone configs should still exist.
	var buf []byte
	cfg := &zonepb.ZoneConfig{}
	sqlDB.QueryRow(t, "SELECT config FROM system.zones WHERE id = $1", tableDesc.ID).Scan(&buf)
	if err := protoutil.Unmarshal(buf, cfg); err != nil {
		t.Fatal(err)
	}

	subzones := []struct {
		index     uint32
		partition string
	}{
		{1, ""},
		{3, ""},
		{3, "p2"},
	}
	for _, target := range subzones {
		if exists := subzoneExists(cfg, target.index, target.partition); !exists {
			t.Fatalf(`zone config for %v does not exist`, target)
		}
	}
	// Dropped indexes waiting for GC shouldn't have their zone configs be visible
	// using SHOW ZONE CONFIGURATIONS ..., but still need to exist in system.zones.
	for _, target := range []string{"t.kv@i", "t.kv.p2"} {
		if exists := sqlutils.ZoneConfigExists(t, sqlDB, target); exists {
			t.Fatalf(`zone config for %s still exists`, target)
		}
	}
	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, _, err := tableDesc.FindIndexByName("i"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
	close(asyncNotification)

	// Wait for index drop to complete so zone configs are updated.
	testutils.SucceedsSoon(t, func() error {
		if kvs, err := kvDB.Scan(context.Background(), indexSpan.Key, indexSpan.EndKey, 0); err != nil {
			return err
		} else if l := 0; len(kvs) != l {
			return errors.Errorf("expected %d key value pairs, but got %d", l, len(kvs))
		}
		sqlDB.QueryRow(t, "SELECT config FROM system.zones WHERE id = $1", tableDesc.ID).Scan(&buf)
		if err := protoutil.Unmarshal(buf, cfg); err != nil {
			return err
		}
		if exists := subzoneExists(cfg, 1, ""); !exists {
			return errors.New("zone config for primary index removed after dropping secondary index")
		}
		for _, target := range subzones[1:] {
			if exists := subzoneExists(cfg, target.index, target.partition); exists {
				return fmt.Errorf(`zone config for %v still exists`, target)
			}
		}
		return nil
	})
}
