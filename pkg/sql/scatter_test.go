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
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestScatterRandomizeLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "uses too many resources for stressrace")
	skip.UnderShort(t, "takes 25s")

	const numHosts = 3

	tc := serverutils.StartNewTestCluster(t, numHosts, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t",
		"k INT PRIMARY KEY, v INT",
		1000,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(10)),
	)

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Even though we disabled merges via the store testing knob, we must also
	// disable the setting in order for manual splits to be allowed.
	r.Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

	// Introduce 99 splits to get 100 ranges.
	r.Exec(t, "ALTER TABLE test.t SPLIT AT (SELECT i*10 FROM generate_series(1, 99) AS g(i))")

	getLeaseholders := func() (map[int]int, error) {
		rows := r.Query(t, `SELECT range_id, lease_holder FROM [SHOW RANGES FROM TABLE test.t]`)
		leaseholders := make(map[int]int)
		numRows := 0
		for ; rows.Next(); numRows++ {
			var rangeID, leaseholder int
			if err := rows.Scan(&rangeID, &leaseholder); err != nil {
				return nil, err
			}
			if rangeID < 1 {
				t.Fatalf("invalid rangeID: %d", rangeID)
			}
			if leaseholder < 1 || leaseholder > numHosts {
				return nil, fmt.Errorf("invalid lease_holder value: %d", leaseholder)
			}
			leaseholders[rangeID] = leaseholder
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if numRows != 100 {
			return nil, fmt.Errorf("expected 100 ranges, got %d", numRows)
		}
		return leaseholders, nil
	}

	oldLeaseholders, err := getLeaseholders()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		// Ensure that scattering changes the leaseholders, which is really all
		// that randomizing the lease placements can probabilistically guarantee -
		// it doesn't guarantee a uniform distribution.
		r.Exec(t, "ALTER TABLE test.t SCATTER")
		newLeaseholders, err := getLeaseholders()
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(oldLeaseholders, newLeaseholders) {
			t.Errorf("expected scatter to change lease distribution, but got no change: %v", newLeaseholders)
		}
		oldLeaseholders = newLeaseholders
	}
}

// TestScatterResponse ensures that ALTER TABLE... SCATTER includes one row of
// output per range in the table. It does *not* test that scatter properly
// distributes replicas and leases; see TestScatter for that.
//
// TODO(benesch): consider folding this test into TestScatter once TestScatter
// is unskipped.
func TestScatterResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTable(
		t, sqlDB, "t",
		"k INT PRIMARY KEY, v INT",
		1000,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(10)),
	)
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, "ALTER TABLE test.t SPLIT AT (SELECT i*10 FROM generate_series(1, 99) AS g(i))")
	rows := r.Query(t, "ALTER TABLE test.t SCATTER")

	i := 0
	for ; rows.Next(); i++ {
		var actualKey []byte
		var pretty string
		if err := rows.Scan(&actualKey, &pretty); err != nil {
			t.Fatal(err)
		}
		var expectedKey roachpb.Key
		if i == 0 {
			expectedKey = keys.SystemSQLCodec.TablePrefix(uint32(tableDesc.GetID()))
		} else {
			var err error
			expectedKey, err = randgen.TestingMakePrimaryIndexKey(tableDesc, i*10)
			if err != nil {
				t.Fatal(err)
			}
		}
		if e, a := expectedKey, roachpb.Key(actualKey); !e.Equal(a) {
			t.Errorf("%d: expected split key %s, but got %s", i, e, a)
		}
		if e, a := expectedKey.String(), pretty; e != a {
			t.Errorf("%d: expected pretty split key %s, but got %s", i, e, a)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if e, a := 100, i; e != a {
		t.Fatalf("expected %d rows, but got %d", e, a)
	}
}
