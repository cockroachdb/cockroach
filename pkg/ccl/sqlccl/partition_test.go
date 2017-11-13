// Copyright 2017 The Cockroach Authors.
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

package sqlccl_test

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func verifyScansOnNode(db *gosql.DB, query string, node string) error {
	rows, err := db.Query(
		fmt.Sprintf(`SELECT context, message FROM [SHOW TRACE FOR %s]`, query),
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	var scansWrongNode []string
	var traceLines []string
	var context, message gosql.NullString
	for rows.Next() {
		if err := rows.Scan(&context, &message); err != nil {
			return err
		}
		traceLine := fmt.Sprintf("%s %s", context.String, message.String)
		traceLines = append(traceLines, traceLine)
		if strings.Contains(message.String, "read completed") && !strings.Contains(context.String, node) {
			scansWrongNode = append(scansWrongNode, traceLine)
		}
	}
	if len(scansWrongNode) > 0 {
		var err bytes.Buffer
		fmt.Fprintf(&err, "expected scans on %s:\n%s\nfull trace:", node, strings.Join(scansWrongNode, "\n"))
		for _, traceLine := range traceLines {
			err.WriteString("\n  ")
			err.WriteString(traceLine)
		}
		return errors.New(err.String())
	}
	return nil
}

func TestPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg := config.DefaultZoneConfig()
	cfg.NumReplicas = 1
	defer config.TestingSetDefaultZoneConfig(cfg)()

	tsArgs := func(attr string) base.TestServerArgs {
		return base.TestServerArgs{
			ScanInterval: time.Second,
			StoreSpecs: []base.StoreSpec{
				{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{attr}}},
			},
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs("dc0"),
		1: tsArgs("dc1"),
		2: tsArgs("dc2"),
	}}
	tc := testcluster.StartTestCluster(t, 3, tcArgs)
	defer tc.Stopper().Stop(context.Background())

	type testScan = struct {
		where         string
		expectedNodes string
	}

	type testCase struct {
		name  string
		table string
		scans []testScan
	}

	testCases := []testCase{
		{
			name: "list/int",
			table: `CREATE TABLE %s (
				a INT, b INT, PRIMARY KEY (a, b)
			) PARTITION BY LIST (a) (
				PARTITION dc1 VALUES IN (10),
				PARTITION dc2 VALUES IN (20)
			)`,
			scans: []testScan{{"a = 10", "n2"}, {"a = 20", "n3"}},
		},
		{
			name: "list/string",
			table: `CREATE TABLE %s (
				a STRING, b STRING, PRIMARY KEY (a, b)
			) PARTITION BY LIST (a) (
				PARTITION dc1 VALUES IN ('a'),
				PARTITION dc2 VALUES IN ('b')
			)`,
			scans: []testScan{{"a = 'a'", "n2"}, {"a = 'b'", "n3"}},
		},
		{
			name: "range/int",
			table: `CREATE TABLE %s (
				a INT, b INT, PRIMARY KEY (a, b)
			) PARTITION BY RANGE (a) (
				PARTITION dc1 VALUES < 4,
				PARTITION dc2 VALUES < 8
			)`,
			scans: []testScan{{"a = 3", "n2"}, {"a = 7", "n3"}},
		},
		{
			name: "range/string",
			table: `CREATE TABLE %s (
				a STRING, b STRING, PRIMARY KEY (a, b)
			) PARTITION BY RANGE (a) (
				PARTITION dc1 VALUES < 'a',
				PARTITION dc2 VALUES < 'ac'
			)`,
			scans: []testScan{{"a = 'A'", "n2"}, {"a = 'ab'", "n3"}},
		},
	}

	if _, err := tc.Conns[0].Exec(`CREATE DATABASE data`); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(t, tc.Conns[0])
			tableName := fmt.Sprintf("data.%q", testCase.name)
			sqlDB.Exec(fmt.Sprintf(testCase.table, tableName))
			sqlDB.Exec(fmt.Sprintf(
				`ALTER TABLE %s PARTITION dc1 EXPERIMENTAL CONFIGURE ZONE 'constraints: [+dc1]'`, tableName))
			sqlDB.Exec(fmt.Sprintf(
				`ALTER TABLE %s PARTITION dc2 EXPERIMENTAL CONFIGURE ZONE 'constraints: [+dc2]'`, tableName))
			testutils.SucceedsSoon(t, func() error {
				for _, scan := range testCase.scans {
					query := fmt.Sprintf(`SELECT * FROM %s WHERE %s`, tableName, scan.where)
					fmt.Printf("query is %s\n", query)
					if err := verifyScansOnNode(sqlDB.DB, query, scan.expectedNodes); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}
}
