// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

func TestShowTraceReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 34213)

	const numNodes = 4

	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.NumReplicas = proto.Int32(1)

	ctx := context.Background()
	tsArgs := func(node string) base.TestServerArgs {
		return base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride:       &zoneConfig,
					DefaultSystemZoneConfigOverride: &zoneConfig,
				},
			},
			StoreSpecs: []base.StoreSpec{{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{node}}}},
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs(`n1`),
		1: tsArgs(`n2`),
		2: tsArgs(`n3`),
		3: tsArgs(`n4`),
	}}
	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `ALTER RANGE "default" CONFIGURE ZONE USING constraints = '[+n4]'`)
	sqlDB.Exec(t, `ALTER DATABASE system CONFIGURE ZONE USING constraints = '[+n4]'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t1 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.t2 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.t3 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE d.t1 CONFIGURE ZONE USING constraints = '[+n1]'`)
	sqlDB.Exec(t, `ALTER TABLE d.t2 CONFIGURE ZONE USING constraints = '[+n2]'`)
	sqlDB.Exec(t, `ALTER TABLE d.t3 CONFIGURE ZONE USING constraints = '[+n3]'`)

	tests := []struct {
		query    string
		expected [][]string
		distinct bool
	}{
		{
			// Read-only
			query:    `SELECT * FROM d.t1`,
			expected: [][]string{{`1`, `1`}},
		},
		{
			// Write-only
			query:    `UPSERT INTO d.t2 VALUES (1)`,
			expected: [][]string{{`2`, `2`}},
		},
		{
			// A write to delete the row.
			query:    `DELETE FROM d.t2`,
			expected: [][]string{{`2`, `2`}},
		},
		{
			// Admin command. We use distinct because the ALTER statement is
			// DDL and cause event log / job ranges to be touched too.
			query:    `ALTER TABLE d.t3 SCATTER`,
			expected: [][]string{{`4`, `4`}, {`3`, `3`}},
			distinct: true,
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			testutils.SucceedsSoon(t, func() error {
				_ = sqlDB.Exec(t, fmt.Sprintf(`SET tracing = on; %s; SET tracing = off`, test.query))

				distinct := ""
				if test.distinct {
					distinct = "DISTINCT"
				}
				actual := sqlDB.QueryStr(t,
					fmt.Sprintf(`SELECT %s node_id, store_id FROM [SHOW EXPERIMENTAL_REPLICA TRACE FOR SESSION]`, distinct),
				)
				if !reflect.DeepEqual(actual, test.expected) {
					return errors.Errorf(`%s: got %v expected %v`, test.query, actual, test.expected)
				}
				return nil
			})
		})
	}
}
