// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestFetchTableDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, stopServer := startTestFullServer(t, feedTestOptions{})
	defer stopServer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, "CREATE TABLE foo (a INT, b STRING)")
	ts := s.Clock().Now()
	ctx := context.Background()

	fooDescr := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "d", "foo")
	var targets changefeedbase.Targets
	targets.Add(changefeedbase.Target{
		TableID: fooDescr.GetID(),
	})

	// Lay protected timestamp record.
	ptr := createProtectedTimestampRecord(ctx, s.Codec(), 42, targets, ts)
	require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return execCfg.ProtectedTimestampProvider.WithTxn(txn).Protect(ctx, ptr)
	}))

	// Alter foo few times, then force GC at ts-1.
	sqlDB.Exec(t, "ALTER TABLE foo ADD COLUMN c STRING")
	sqlDB.Exec(t, "ALTER TABLE foo ADD COLUMN d STRING")
	require.NoError(t, s.ForceTableGC(ctx, "system", "descriptor", ts.Add(-1, 0)))

	// We can still fetch table descriptors because of protected timestamp record.
	asOf := ts
	_, err := fetchTableDescriptors(ctx, &execCfg, targets, asOf)
	require.NoError(t, err)
}
