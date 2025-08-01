// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"math/rand"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
)

// runIndexUpgrade runs a test that creates an index before a version upgrade,
// and modifies it in a mixed version setting. It aims to test the changes made
// to index encodings done to allow secondary indexes to respect column families.
func runIndexUpgrade(ctx context.Context, t test.Test, c cluster.Cluster) {
	firstExpected := [][]string{
		{"2", "3", "4"},
		{"6", "7", "8"},
		{"10", "11", "12"},
		{"14", "15", "17"},
	}
	secondExpected := [][]string{
		{"2", "3", "4"},
		{"6", "7", "8"},
		{"10", "11", "12"},
		{"14", "15", "17"},
		{"21", "25", "25"},
	}

	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(),
		mixedversion.NeverUseFixtures, mixedversion.AlwaysUseLatestPredecessors,
	)
	mvt.OnStartup(
		"fill the cluster with data",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			if err := h.Exec(r, `
CREATE TABLE t (
	x INT PRIMARY KEY, y INT, z INT, w INT,
	INDEX i (y) STORING (z, w),
	FAMILY (x), FAMILY (y), FAMILY (z), FAMILY (w)
);
INSERT INTO t VALUES (1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12);
`); err != nil {
				return err
			}
			return nil
		})

	mvt.InMixedVersion(
		"modify and verify index data while in a mixed version state",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			// Run the following statements in a node running the next
			// version, if any; otherwise, pick a random node.
			nodes := c.All()
			if h.Context().MixedBinary() {
				nodes = h.Context().NodesInNextVersion()
			}

			if err := h.ExecWithGateway(r, nodes, `DELETE FROM t WHERE x = 13 OR x = 20`); err != nil {
				return err
			}
			if err := h.ExecWithGateway(r, nodes, `INSERT INTO t VALUES (13, 14, 15, 16)`); err != nil {
				return err
			}
			if err := h.ExecWithGateway(r, nodes, `UPDATE t SET w = 17 WHERE y = 14`); err != nil {
				return err
			}
			if err := verifyTableData(ctx, c, l, 1, firstExpected); err != nil {
				return err
			}
			if err := verifyTableData(ctx, c, l, 2, firstExpected); err != nil {
				return err
			}
			if err := verifyTableData(ctx, c, l, 3, firstExpected); err != nil {
				return err
			}
			return nil
		},
	)

	mvt.AfterUpgradeFinalized(
		"modify more data after the upgade and verify data",
		func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
			if err := h.Exec(r, `DELETE FROM t WHERE x = 20`); err != nil {
				return err
			}
			if err := h.Exec(r, `INSERT INTO t VALUES (20, 21, 22, 23)`); err != nil {
				return err
			}
			if err := h.Exec(r, `UPDATE t SET w = 25, z = 25 WHERE y = 21`); err != nil {
				return err
			}
			if err := verifyTableData(ctx, c, l, 1, secondExpected); err != nil {
				return err
			}
			if err := verifyTableData(ctx, c, l, 2, secondExpected); err != nil {
				return err
			}
			if err := verifyTableData(ctx, c, l, 3, secondExpected); err != nil {
				return err
			}
			return nil
		},
	)

	mvt.Run()
}

func verifyTableData(
	ctx context.Context, c cluster.Cluster, l *logger.Logger, node int, expected [][]string,
) (retErr error) {
	conn := c.Conn(ctx, l, node)
	defer func() { retErr = errors.CombineErrors(retErr, conn.Close()) }()
	rows, err := conn.Query(`SELECT y, z, w FROM t@i ORDER BY y`)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
	actual, err := sqlutils.RowsToStrMatrix(rows)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return errors.Errorf("expected %v, got %v", expected, actual)
	}
	return nil
}

func registerSecondaryIndexesMultiVersionCluster(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "schemachange/secondary-index-multi-version",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(3),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runIndexUpgrade(ctx, t, c)
		},
	})
}
