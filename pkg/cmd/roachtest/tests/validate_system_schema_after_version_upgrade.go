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
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
)

// This test tests that, after bootstrapping a cluster from a previous
// release's binary and upgrading it to the latest version, the `system`
// database "contains the expected tables".
// Specifically, we do the check with `USE system; SHOW CREATE ALL TABLES;`
// and assert that the output matches the expected output content.
func runValidateSystemSchemaAfterVersionUpgrade(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	// Obtain system table definitions with `SHOW CREATE ALL TABLES` in the SYSTEM db.
	obtainSystemSchema := func(ctx context.Context, l *logger.Logger, c cluster.Cluster, node int) string {
		// Create a connection to the database cluster.
		db := c.Conn(ctx, l, node)
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

	// Start a cluster with the latest binary and get the system schema from the
	// cluster.
	if err := clusterupgrade.StartWithSettings(
		ctx, t.L(), c, c.All(), option.DefaultStartOpts(), install.BinaryOption(test.DefaultCockroachPath),
	); err != nil {
		t.Fatal(err)
	}
	expected = obtainSystemSchema(ctx, t.L(), c, 1)
	c.Wipe(ctx, false /* preserveCerts */, c.All())

	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(),
		// Fixtures are generated on a version that's too old for this test.
		mixedversion.NeverUseFixtures,
		// We limit the number of upgrades since the test is not expected to work
		// on versions older than 22.2.
		mixedversion.MaxUpgrades(2),
	)
	mvt.AfterUpgradeFinalized(
		"obtain system schema from the upgraded cluster",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			if !h.Context().ToVersion.IsCurrent() {
				// Only validate the system schema if we're upgrading to the version
				// under test.
				return nil
			}

			// Compare whether the two schemas are equal
			actual = obtainSystemSchema(ctx, l, c, 1)
			if expected != actual {
				diff, diffErr := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:       difflib.SplitLines(expected),
					B:       difflib.SplitLines(actual),
					Context: 5,
				})
				if diffErr != nil {
					return errors.Wrap(diffErr, "failed to produce diff")
				}
				return errors.Newf("After upgrading, `USE system; SHOW CREATE ALL TABLES;` "+
					"does not match expected output after version upgrade."+
					"\nDiff:\n%s", diff)
			}
			l.Printf("validating succeeded:\n%v", expected)
			return nil
		},
	)
	mvt.Run()
}
