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

package sql_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestShowTraceReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numNodes = 3

	cfg := config.DefaultZoneConfig()
	cfg.NumReplicas = 1
	defer config.TestingSetDefaultZoneConfig(cfg)()

	ctx := context.Background()
	tsArgs := func(node string) base.TestServerArgs {
		return base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{node}}},
			},
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs(`n1`),
		1: tsArgs(`n2`),
		2: tsArgs(`n3`),
	}}
	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.public.t1 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.public.t2 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.public.t3 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE d.public.t1 EXPERIMENTAL CONFIGURE ZONE 'constraints: [+n1]'`)
	sqlDB.Exec(t, `ALTER TABLE d.public.t2 EXPERIMENTAL CONFIGURE ZONE 'constraints: [+n2]'`)
	sqlDB.Exec(t, `ALTER TABLE d.public.t3 EXPERIMENTAL CONFIGURE ZONE 'constraints: [+n3]'`)

	tests := []struct {
		query    string
		expected [][]string
	}{
		{
			// Read-only
			query:    `SELECT * FROM d.public.t1`,
			expected: [][]string{{`1`, `1`}},
		},
		{
			// Write-only
			query:    `UPSERT INTO d.public.t2 VALUES (1)`,
			expected: [][]string{{`2`, `2`}},
		},
		{
			// First a read to compute the deletions then a write to delete them.
			query:    `DELETE FROM d.public.t2`,
			expected: [][]string{{`2`, `2`}, {`2`, `2`}},
		},
		{
			// Admin command
			query:    `ALTER TABLE d.public.t3 SCATTER`,
			expected: [][]string{{`3`, `3`}},
		},
	}

	testutils.SucceedsSoon(t, func() error {
		for _, test := range tests {
			query := fmt.Sprintf(
				`SELECT node_id, store_id FROM [SHOW EXPERIMENTAL_REPLICA TRACE FOR %s]`,
				test.query)
			actual := sqlDB.QueryStr(t, query)
			if !reflect.DeepEqual(actual, test.expected) {
				return errors.Errorf(`%s: got %v expected %v`, test.query, actual, test.expected)
			}
		}
		return nil
	})
}
