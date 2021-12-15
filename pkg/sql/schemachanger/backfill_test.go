// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachanger

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBackfillCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")

	tdb.Exec(t, "SET experimental_use_new_schema_changer = unsafe_always")
	require.NoError(t, crdb.ExecuteTx(ctx, tc.ServerConn(0), nil, func(tx *gosql.Tx) error {
		_, err := tx.Exec("ALTER TABLE foo ADD COLUMN j INT NOT NULL DEFAULT 42")
		return err
	}))
}
