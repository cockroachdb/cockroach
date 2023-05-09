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
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/pmezard/go-difflib/difflib"
)

func registerValidateSystemSchemaAfterVersionUpgrade(r registry.Registry) {
	// This test tests that, after bootstrapping a cluster from a previous
	// release's binary and upgrading it to the latest version, the `system`
	// database "contains the expected tables".
	// Specifically, we do the check with `USE system; SHOW CREATE ALL TABLES;`
	// and assert that the output matches the expected output content.
	r.Add(registry.TestSpec{
		Name:    "systemschema/validate-after-version-upgrade",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(1),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() && runtime.GOARCH == "arm64" {
				t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
			}
			predecessorVersion, err := version.PredecessorVersion(*t.BuildVersion())
			if err != nil {
				t.Fatal(err)
			}

			// Obtain system table definitions with `SHOW CREATE ALL TABLES` in the SYSTEM db.
			obtainSystemSchema := func(ctx context.Context, t test.Test, u *versionUpgradeTest, node int) string {
				// Create a connection to the database cluster.
				db := u.conn(ctx, t, node)
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

				return sb.String()
			}

			// expected and actual output of `SHOW CREATE ALL TABLES;`.
			var expected, actual string

			// Query node `SHOW CREATE ALL TABLES` and store return in output.
			obtainSystemSchemaStep := func(node int, output *string) versionStep {
				return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
					*output = obtainSystemSchema(ctx, t, u, node)
				}
			}

			// Wipe nodes in this test's cluster.
			wipeClusterStep := func(nodes option.NodeListOption) versionStep {
				return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
					u.c.Wipe(ctx, nodes)
				}
			}

			// Compare whether two strings are equal -- used to compare expected and actual.
			validateEquivalenceStep := func(str1, str2 *string) versionStep {
				return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
					if *str1 != *str2 {
						diff, diffErr := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
							A:       difflib.SplitLines(*str1),
							B:       difflib.SplitLines(*str2),
							Context: 5,
						})
						if diffErr != nil {
							diff = diffErr.Error()
							t.Errorf("failed to produce diff: %v", diffErr)
						}
						t.Fatalf("After upgrading, `USE system; SHOW CREATE ALL TABLES;` "+
							"does not match expected output after version upgrade."+
							"\nDiff:\n%s", diff)
					}
					t.L().Printf("validating succeeded:\n%v", *str1)
				}
			}

			u := newVersionUpgradeTest(c,
				// Start the node with the latest binary version.
				uploadAndStart(c.Node(1), clusterupgrade.MainVersion),

				// Obtain expected output from the node.
				obtainSystemSchemaStep(1, &expected),

				// Wipe the node.
				wipeClusterStep(c.Node(1)),

				// Restart the node with a previous binary version.
				uploadAndStart(c.Node(1), predecessorVersion),

				// Upgrade the node version.
				binaryUpgradeStep(c.Node(1), clusterupgrade.MainVersion),

				// Wait for the cluster version to also bump up to make sure the migration logic is run.
				waitForUpgradeStep(c.Node(1)),

				// Obtain the actual output on the upgraded version.
				obtainSystemSchemaStep(1, &actual),

				// Compare the results.
				validateEquivalenceStep(&expected, &actual),
			)
			u.run(ctx, t)
		},
	})
}
