// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Test the behavior of a binary that doesn't link in CCL when it comes to
// dealing with partitions. Some things are expected to work, others aren't.
func TestRemovePartitioningOSS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(ctx)

	const numRows = 100
	if err := tests.CreateKVTable(sqlDBRaw, "kv", numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	tableKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID)

	// Hack in partitions. Doing this properly requires a CCL binary.
	{
		primaryIndex := *tableDesc.GetPrimaryIndex().IndexDesc()
		primaryIndex.Partitioning = descpb.PartitioningDescriptor{
			NumColumns: 1,
			Range: []descpb.PartitioningDescriptor_Range{{
				Name:          "p1",
				FromInclusive: encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 1),
				ToExclusive:   encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 2),
			}},
		}
		tableDesc.SetPrimaryIndex(primaryIndex)
	}

	{
		secondaryIndex := *tableDesc.PublicNonPrimaryIndexes()[0].IndexDesc()
		secondaryIndex.Partitioning = descpb.PartitioningDescriptor{
			NumColumns: 1,
			Range: []descpb.PartitioningDescriptor_Range{{
				Name:          "p2",
				FromInclusive: encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 1),
				ToExclusive:   encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 2),
			}},
		}
		tableDesc.SetPublicNonPrimaryIndex(1, secondaryIndex)
	}
	// Note that this is really a gross hack - it breaks planner caches, which
	// assume that nothing is going to change out from under them like this. We
	// "fix" the issue by altering the table's name to refresh the cache, below.
	if err := kvDB.Put(ctx, tableKey, tableDesc.DescriptorProto()); err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, "ALTER TABLE t.kv RENAME to t.kv2")
	sqlDB.Exec(t, "ALTER TABLE t.kv2 RENAME to t.kv")
	exp := `CREATE TABLE public.kv (
	k INT8 NOT NULL,
	v INT8 NULL,
	CONSTRAINT "primary" PRIMARY KEY (k ASC),
	INDEX foo (v ASC) PARTITION BY RANGE (v) (
		PARTITION p2 VALUES FROM (1) TO (2)
	),
	FAMILY fam_0_k (k),
	FAMILY fam_1_v (v)
) PARTITION BY RANGE (k) (
	PARTITION p1 VALUES FROM (1) TO (2)
)
-- Warning: Partitioned table with no zone configurations.`
	if a := sqlDB.QueryStr(t, "SHOW CREATE t.kv")[0][1]; exp != a {
		t.Fatalf("expected:\n%s\n\ngot:\n%s\n\n", exp, a)
	}

	// Hack in partition zone configs. This also requires a CCL binary to do
	// properly.
	zoneConfig := zonepb.ZoneConfig{
		Subzones: []zonepb.Subzone{
			{
				IndexID:       uint32(tableDesc.GetPrimaryIndexID()),
				PartitionName: "p1",
				Config:        s.(*server.TestServer).Cfg.DefaultZoneConfig,
			},
			{
				IndexID:       uint32(tableDesc.PublicNonPrimaryIndexes()[0].GetID()),
				PartitionName: "p2",
				Config:        s.(*server.TestServer).Cfg.DefaultZoneConfig,
			},
		},
	}
	zoneConfigBytes, err := protoutil.Marshal(&zoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, zoneConfigBytes)
	for _, p := range []string{
		"PARTITION p1 OF INDEX t.public.kv@primary",
		"PARTITION p2 OF INDEX t.public.kv@foo",
	} {
		if exists := sqlutils.ZoneConfigExists(t, sqlDB, p); !exists {
			t.Fatalf("zone config for %s does not exist", p)
		}
	}

	// Some things don't work.
	sqlDB.ExpectErr(t,
		"OSS binaries do not include enterprise features",
		`ALTER PARTITION p1 OF TABLE t.kv CONFIGURE ZONE USING DEFAULT`)
	sqlDB.ExpectErr(t,
		"OSS binaries do not include enterprise features",
		`ALTER PARTITION p2 OF INDEX t.kv@foo CONFIGURE ZONE USING DEFAULT`)

	// But removing partitioning works.
	sqlDB.Exec(t, `ALTER TABLE t.kv PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t.kv@foo PARTITION BY NOTHING`)
	sqlDB.Exec(t, `DELETE FROM system.zones WHERE id = $1`, tableDesc.ID)
	sqlDB.Exec(t, `ALTER TABLE t.kv PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t.kv@foo PARTITION BY NOTHING`)

	exp = `CREATE TABLE public.kv (
	k INT8 NOT NULL,
	v INT8 NULL,
	CONSTRAINT "primary" PRIMARY KEY (k ASC),
	INDEX foo (v ASC),
	FAMILY fam_0_k (k),
	FAMILY fam_1_v (v)
)`
	if a := sqlDB.QueryStr(t, "SHOW CREATE t.kv")[0][1]; exp != a {
		t.Fatalf("expected:\n%s\n\ngot:\n%s\n\n", exp, a)
	}
}
