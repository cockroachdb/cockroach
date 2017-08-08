// Copyright 2016 The Cockroach Authors.
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
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSplitAt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	r := sqlutils.MakeSQLRunner(t, db)

	r.Exec("CREATE DATABASE d")
	r.Exec(`CREATE TABLE d.t (
		i INT,
		s STRING,
		PRIMARY KEY (i, s),
		INDEX s_idx (s)
	)`)
	r.Exec(`CREATE TABLE d.i (k INT PRIMARY KEY)`)

	tests := []struct {
		in    string
		error string
		args  []interface{}
	}{
		{
			in: "ALTER TABLE d.t SPLIT AT VALUES (2, 'b')",
		},
		{
			// Splitting at an existing split is a silent no-op.
			in: "ALTER TABLE d.t SPLIT AT VALUES (2, 'b')",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT VALUES (3, 'c'), (4, 'd')",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT SELECT 5, 'd'",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT SELECT * FROM (VALUES (6, 'e'), (7, 'f')) AS a",
		},
		{
			in: "ALTER TABLE d.t SPLIT AT VALUES (10)",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT VALUES ('c', 3)",
			error: "SPLIT AT data column 1 (i) must be of type int, not type string",
		},
		{
			in:    "ALTER TABLE d.t SPLIT AT VALUES (i, s)",
			error: `name "i" is not defined`,
		},
		{
			in: "ALTER INDEX d.t@s_idx SPLIT AT VALUES ('f')",
		},
		{
			in:    "ALTER INDEX d.t@not_present SPLIT AT VALUES ('g')",
			error: `index "not_present" does not exist`,
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES (avg(1))",
			error: "aggregate functions are not allowed in VALUES",
		},
		{
			in:   "ALTER TABLE d.i SPLIT AT VALUES ($1)",
			args: []interface{}{8},
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES ($1)",
			error: "no value provided for placeholder: $1",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES ($1)",
			args:  []interface{}{"blah"},
			error: "error in argument for $1: strconv.ParseInt",
		},
		{
			in:    "ALTER TABLE d.i SPLIT AT VALUES ($1::string)",
			args:  []interface{}{"1"},
			error: "SPLIT AT data column 1 (k) must be of type int, not type string",
		},
		{
			in: "ALTER TABLE d.i SPLIT AT VALUES ((SELECT 1))",
		},
	}

	for _, tt := range tests {
		var key roachpb.Key
		var pretty string
		err := db.QueryRow(tt.in, tt.args...).Scan(&key, &pretty)
		if err != nil && tt.error == "" {
			t.Fatalf("%s: unexpected error: %s", tt.in, err)
		} else if tt.error != "" && err == nil {
			t.Fatalf("%s: expected error: %s", tt.in, tt.error)
		} else if err != nil && tt.error != "" {
			if !strings.Contains(err.Error(), tt.error) {
				t.Fatalf("%s: unexpected error: %s", tt.in, err)
			}
		} else {
			// Successful split, verify it happened.
			rng, err := s.(*server.TestServer).LookupRange(key)
			if err != nil {
				t.Fatal(err)
			}
			expect := roachpb.Key(rng.StartKey)
			if !expect.Equal(key) {
				t.Fatalf("%s: expected range start %s, got %s", tt.in, expect, pretty)
			}
		}
	}
}

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

	r := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))

	// Introduce 99 splits to get 100 ranges.
	r.Exec("ALTER TABLE test.t SPLIT AT (SELECT i*10 FROM generate_series(1, 99) AS g(i))")

	// Ensure that scattering leaves each node with at least 20% of the leases.
	r.Exec("ALTER TABLE test.t SCATTER")
	rows := r.Query("SHOW TESTING_RANGES FROM TABLE test.t")
	// See showRangesColumns for the schema.
	if cols, err := rows.Columns(); err != nil {
		t.Fatal(err)
	} else if len(cols) != 4 {
		t.Fatalf("expected 4 columns, got %#v", cols)
	}
	vals := []interface{}{
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
		leaseHolder := *vals[3].(*int)
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

	r := sqlutils.MakeSQLRunner(t, sqlDB)
	r.Exec("ALTER TABLE test.t SPLIT AT (SELECT i*10 FROM generate_series(1, 99) AS g(i))")
	rows := r.Query("ALTER TABLE test.t SCATTER")

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
