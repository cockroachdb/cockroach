// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// This test ensures that pg_table_size returns the right number. If it fails,
// maybe you changed the key encoding?? Look out!
func TestPGTableSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	_, err := tc.Conns[0].ExecContext(ctx, `CREATE TABLE t(int) AS SELECT generate_series(0, 1000)`)
	require.NoError(t, err)

	row := tc.Conns[0].QueryRowContext(ctx, `SELECT 't'::regclass::int`)
	require.NoError(t, row.Err())
	var tableID int
	err = row.Scan(&tableID)
	require.NoError(t, err)
	tablePrefix := keys.MakeSQLCodec(tc.Servers[0].Cfg.TenantID).TablePrefix(uint32(tableID))
	require.NoError(t, tc.WaitForSplitAndInitialization(tablePrefix))

	row = tc.Conns[0].QueryRowContext(ctx, `SELECT pg_total_relation_size('t'::regclass)`)
	require.NoError(t, row.Err())
	var actual int
	err = row.Scan(&actual)
	require.NoError(t, err)
	expected := 32969
	if actual != expected {
		t.Errorf("expected pg_table_size(t) to be %d, found %d", expected, actual)
	}
}
