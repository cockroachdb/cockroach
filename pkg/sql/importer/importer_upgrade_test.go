// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestImportMixedVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tests := []struct {
		name        string
		create      string
		typ         string
		with        string
		data        string
		verifyQuery string
		err         string
		expected    [][]string
	}{
		{
			name: "pgdump multiple inserts same table",
			typ:  "PGDUMP",
			data: `CREATE TABLE t (a INT, b INT);
				INSERT INTO t (a, b) VALUES (1, 2);
				INSERT INTO t (a, b) VALUES (3, 4);
				INSERT INTO t (a, b) VALUES (5, 6);
				INSERT INTO t (a, b) VALUES (7, 8);
				`,
			with:        `WITH row_limit = '2'`,
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"1", "2"}, {"3", "4"}},
		},
		// Test Mysql imports.
		{
			name: "mysqldump single table",
			typ:  "MYSQLDUMP",
			data: `CREATE TABLE t (a INT, b INT);
				INSERT INTO t (a, b) VALUES (5, 6), (7, 8);
				`,
			with:        `WITH row_limit = '1'`,
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"5", "6"}},
		},
	}

	tc := serverutils.StartNewTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
				},
			},
		}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	for _, test := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(test.data))
			}
		}))
		importDumpQuery := fmt.Sprintf(`IMPORT TABLE t FROM %s ($1) %s`, test.typ, test.with)

		sqlDB.Exec(t, importDumpQuery, srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * FROM t`, test.expected)

		var parentSchemaID int
		row := sqlDB.QueryRow(t, `SELECT "parentSchemaID" FROM system.namespace WHERE name='t'`)
		row.Scan(&parentSchemaID)

		// We're in the mixed version where databases do not yet have descriptor
		// backed public schemas. We expect the imported table to be in the
		// synthetic public schemas.
		require.Equal(t, parentSchemaID, keys.PublicSchemaIDForBackup)

		srv.Close()
		sqlDB.Exec(t, `DROP TABLE IF EXISTS t`)
	}
}
