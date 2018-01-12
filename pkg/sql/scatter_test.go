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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestScatter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#14955")

	const numHosts = 4
	tc := serverutils.StartTestCluster(t, numHosts, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.TODO())

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t",
		"k INT PRIMARY KEY, v INT",
		1000,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(10)),
	)

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Introduce 99 splits to get 100 ranges.
	r.Exec(t, "ALTER TABLE test.t SPLIT AT (SELECT i*10 FROM generate_series(1, 99) AS g(i))")

	// Ensure that scattering leaves each node with at least 20% of the leases.
	r.Exec(t, "ALTER TABLE test.t SCATTER")
	rows := r.Query(t, "SHOW TESTING_RANGES FROM TABLE test.t")
	// See showRangesColumns for the schema.
	if cols, err := rows.Columns(); err != nil {
		t.Fatal(err)
	} else if len(cols) != 5 {
		t.Fatalf("expected 4 columns, got %#v", cols)
	}
	vals := []interface{}{
		new(interface{}),
		new(interface{}),
		new(interface{}),
		new(interface{}),
		new(int),
	}
	leaseHolders := map[int]int{1: 0, 2: 0, 3: 0, 4: 0}
	numRows := 0
	for ; rows.Next(); numRows++ {
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		leaseHolder := *vals[4].(*int)
		if leaseHolder < 1 || leaseHolder > numHosts {
			t.Fatalf("invalid lease holder value: %d", leaseHolder)
		}
		leaseHolders[leaseHolder]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if numRows != 100 {
		t.Fatalf("expected 100 ranges, got %d", numRows)
	}
	for i, count := range leaseHolders {
		if count < 20 {
			t.Errorf("less than 20 leaseholders on host %d (only %d)", i, count)
		}
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

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTable(
		t, sqlDB, "t",
		"k INT PRIMARY KEY, v INT",
		1000,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(10)),
	)
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")

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
			expectedKey = keys.MakeTablePrefix(uint32(tableDesc.ID))
		} else {
			var err error
			expectedKey, err = sqlbase.MakePrimaryIndexKey(tableDesc, i*10)
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
