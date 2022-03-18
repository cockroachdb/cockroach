// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

func registerValidateSystemSchemaAfterVersionUpgrade(r registry.Registry) {
	// This test tests that, after bootstrapping a cluster from a previous
	// release's binary and upgrading it to the latest version, the `system`
	// database "contains the expected tables".
	// Specifically, we do the check with `USE system; SHOW CREATE ALL TABLES;`
	// and assert that the output matches the expected output content.
	r.Add(registry.TestSpec{
		Name:    "systemschema/validate-after-version-upgrade",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(1),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			const mainVersion = ""

			predecessorVersion, err := PredecessorVersion(*t.BuildVersion())
			if err != nil {
				t.Fatal(err)
			}

			validateSystemSchemaStep := func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
				t.L().Printf("validating")

				// Create a connection to the database cluster.
				db := u.conn(ctx, t, 1)
				sqlRunner := sqlutils.MakeSQLRunner(db)

				// Prepare the SQL query.
				sql := `USE SYSTEM; SHOW CREATE ALL TABLES;`

				// Execute the SQL query.
				rows := sqlRunner.QueryStr(t, sql)

				// Extract return.
				var sb strings.Builder
				for _, row := range rows {
					sb.WriteString(row[0])
					sb.WriteString("\n")
				}

				// Compare return with expected output file content.
				dat, err := os.ReadFile("./pkg/cmd/roachtest/tests/validate_system_schema_after_version_upgrade_expected_output")
				if err != nil {
					t.Fatal(err)
				}
				if string(dat) != sb.String() {
					t.Fatal("After upgrading, `USE system; SHOW CREATE ALL TABLES;` does not match expected output after version upgrade.\n")
				}

				t.L().Printf("validating succeeded.\n")
			}

			u := newVersionUpgradeTest(c,
				// Start a new test cluster with an old version.
				uploadAndStart(c.All(), predecessorVersion),

				// Upgrade all nodes in the cluster to a newer version.
				binaryUpgradeStep(c.All(), mainVersion),

				// Wait for the cluster version to also bump up.
				waitForUpgradeStep(c.All()),

				// Validate that the content of `system` database matches expected content.
				validateSystemSchemaStep,
			)
			u.run(ctx, t)
		},
	})
}
