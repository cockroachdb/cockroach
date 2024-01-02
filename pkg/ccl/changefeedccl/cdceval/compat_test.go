// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCompatRewrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a int, b int, c int)`)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	fooDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	target := changefeedbase.Target{
		TableID: fooDesc.GetID(),
	}
	fooRef := fmt.Sprintf("[%d AS foo]", fooDesc.GetID())

	for _, tc := range []struct {
		oldExpr string
		newExpr string
	}{
		{
			oldExpr: "SELECT * FROM defaultdb.public.FOO",
			newExpr: "SELECT * FROM defaultdb.public.foo",
		},
		{
			oldExpr: "SELECT f.a, f.b, f.c FROM foo AS f WHERE f.c IS NULL",
			newExpr: "SELECT f.a, f.b, f.c FROM foo AS f WHERE f.c IS NULL",
		},
		{
			oldExpr: "SELECT foo.*, cdc_prev() FROM " + fooRef,
			newExpr: "SELECT foo.*, row_to_json((cdc_prev).*) FROM " + fooRef,
		},
		{
			oldExpr: "SELECT foo.*, cdc_prev()->'field' FROM " + fooRef,
			newExpr: "SELECT foo.*, row_to_json((cdc_prev).*)->'field' FROM " + fooRef,
		},
		{
			oldExpr: "SELECT foo.*, cdc_prev() FROM " + fooRef + " WHERE cdc_prev()->>'field' = 'blah'",
			newExpr: "SELECT foo.*, row_to_json((cdc_prev).*) FROM " + fooRef + " WHERE (row_to_json((cdc_prev).*)->>'field') = 'blah'",
		},
		{
			oldExpr: "SELECT foo.*, cdc_is_delete() FROM " + fooRef,
			newExpr: "SELECT foo.*, (event_op() = 'delete') FROM " + fooRef,
		},
		{
			oldExpr: "SELECT foo.*, cdc_is_delete() AS was_deleted FROM " + fooRef,
			newExpr: "SELECT foo.*, (event_op() = 'delete') AS was_deleted FROM " + fooRef,
		},
		{
			oldExpr: "SELECT foo.*, cdc_mvcc_timestamp() FROM " + fooRef,
			newExpr: "SELECT foo.*, crdb_internal_mvcc_timestamp FROM " + fooRef,
		},
	} {
		sc, err := ParseChangefeedExpression(tc.oldExpr)
		require.NoError(t, err)
		sc, err = RewritePreviewExpression(sc)
		require.NoError(t, err)
		require.Equal(t, tc.newExpr, AsStringUnredacted(sc))

		// Parse and plan new expression to make sure it's sane.
		_, err = newEvaluatorWithNormCheck(&execCfg, fooDesc, s.Clock().Now(), target, AsStringUnredacted(sc))
		require.NoError(t, err)
	}
}
