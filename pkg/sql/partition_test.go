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
// permissions and limitations under the License.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func TestRemovePartitioning(t *testing.T) {
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(ctx)

	const numRows = 100
	if err := tests.CreateKVTable(sqlDB.DB, numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	tableKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)

	// Hack in partitions. Doing this properly requires a CCL binary.
	tableDesc.PrimaryIndex.Partitioning = sqlbase.PartitioningDescriptor{
		NumColumns: 1,
		Range: []sqlbase.PartitioningDescriptor_Range{{
			Name:          "p1",
			FromInclusive: encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 1),
			ToExclusive:   encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 2),
		}},
	}
	tableDesc.Indexes[0].Partitioning = sqlbase.PartitioningDescriptor{
		NumColumns: 1,
		Range: []sqlbase.PartitioningDescriptor_Range{{
			Name:          "p2",
			FromInclusive: encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 1),
			ToExclusive:   encoding.EncodeIntValue(nil /* appendTo */, encoding.NoColumnID, 2),
		}},
	}
	if err := kvDB.Put(ctx, tableKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		t.Fatal(err)
	}
	exp := `CREATE TABLE kv (
		k INT NOT NULL,
		v INT NULL,
		CONSTRAINT "primary" PRIMARY KEY (k ASC),
		INDEX foo (v ASC) PARTITION BY RANGE (v) (
			PARTITION p2 VALUES FROM (1) TO (2)
		),
		FAMILY fam_0_k (k),
		FAMILY fam_1_v (v)
	) PARTITION BY RANGE (k) (
		PARTITION p1 VALUES FROM (1) TO (2)
	)`
	if showCreate := sqlDB.QueryStr(t, "SHOW CREATE TABLE t.kv")[0][1]; showCreate != 
	fmt.Printf("yikes; %s\n", sqlDB.QueryStr(t, "SHOW CREATE TABLE t.kv")[0])

	// Hack in partition zone configs. This also requires a CCL binary to do
	// properly.
	zoneConfig := config.ZoneConfig{
		Subzones: []config.Subzone{
			{
				IndexID:       uint32(tableDesc.PrimaryIndex.ID),
				PartitionName: "p1",
				Config:        config.DefaultZoneConfig(),
			},
			{
				IndexID:       uint32(tableDesc.Indexes[0].ID),
				PartitionName: "p2",
				Config:        config.DefaultZoneConfig(),
			},
		},
	}
	zoneConfigBytes, err := protoutil.Marshal(&zoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, zoneConfigBytes)
	for _, p := range []string{"p1", "p2"} {
		if exists := sqlutils.ZoneConfigExists(t, sqlDB, "t.kv."+p); !exists {
			t.Fatalf("zone config for %s does not exist", p)
		}
	}

	sqlDB.Exec(t, `ALTER TABLE t.kv PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t.kv@foo PARTITION BY NOTHING`)
}
