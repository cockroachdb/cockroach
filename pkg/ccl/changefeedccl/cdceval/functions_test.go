// Copyright 2022 The Cockroach Authors.
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
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestEvaluatesCDCFunctionOverloads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, ""+
		"CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d INT, "+
		"FAMILY most (a,b,c), FAMILY only_d (d))")
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), kvDB, "foo")

	ctx := context.Background()

	t.Run("current_timestamp", func(t *testing.T) {
		testRow := cdcevent.TestingMakeEventRow(desc, 0, randEncDatumRow(t, desc, 0), false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			"SELECT current_timestamp::int")
		require.NoError(t, err)
		futureTS := s.Clock().Now().Add(int64(60*time.Minute), 0)
		p, err := e.evalProjection(ctx, testRow, futureTS, testRow)
		require.NoError(t, err)
		require.Equal(t,
			map[string]string{"current_timestamp": strconv.FormatInt(futureTS.GoTime().Unix(), 10)},
			slurpValues(t, p))
	})

	t.Run("cdc_is_delete", func(t *testing.T) {
		for _, expectDelete := range []bool{true, false} {
			testRow := cdcevent.TestingMakeEventRow(desc, 0, randEncDatumRow(t, desc, 0), expectDelete)
			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
				"SELECT cdc_is_delete()")
			require.NoError(t, err)

			p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), testRow)
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{"cdc_is_delete": fmt.Sprintf("%t", expectDelete)},
				slurpValues(t, p))
		}
	})

	t.Run("cdc_prev", func(t *testing.T) {
		rowDatums := randEncDatumRow(t, desc, 0)
		testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			"SELECT cdc_prev()")
		require.NoError(t, err)

		// When previous row is not set -- i.e. if running without diff, cdc_prev returns
		// null json.
		p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"cdc_prev": jsonb.NullJSONValue.String()}, slurpValues(t, p))

		// Otherwise, expect to get JSONB.
		b := jsonb.NewObjectBuilder(len(rowDatums))
		for i, d := range rowDatums {
			j, err := tree.AsJSON(d.Datum, sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				require.NoError(t, err)
			}
			b.Add(desc.PublicColumns()[i].GetName(), j)
		}

		expectedJSON := b.Build()
		p, err = e.evalProjection(ctx, testRow, s.Clock().Now(), testRow)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"cdc_prev": expectedJSON.String()}, slurpValues(t, p))
	})

	for _, cast := range []string{"", "::decimal", "::string"} {
		t.Run(fmt.Sprintf("cdc_{mvcc,updated}_timestamp()%s", cast), func(t *testing.T) {
			schemaTS := s.Clock().Now().Add(int64(60*time.Minute), 0)
			mvccTS := schemaTS.Add(int64(30*time.Minute), 0)
			testRow := cdcevent.TestingMakeEventRow(desc, 0, randEncDatumRow(t, desc, 0), false)
			testRow.EventDescriptor.SchemaTS = schemaTS

			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
				fmt.Sprintf(
					"SELECT cdc_mvcc_timestamp()%[1]s as mvcc, cdc_updated_timestamp()%[1]s as updated", cast,
				))
			require.NoError(t, err)

			p, err := e.evalProjection(ctx, testRow, mvccTS, testRow)
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{
					"mvcc":    eval.TimestampToDecimalDatum(mvccTS).String(),
					"updated": eval.TimestampToDecimalDatum(schemaTS).String(),
				},
				slurpValues(t, p))
		})
	}
}
