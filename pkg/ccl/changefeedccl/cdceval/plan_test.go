// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdceval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCanPlanCDCExpressions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.ExecMultiple(t,
		`CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`,
		`CREATE SCHEMA alt`,
		`CREATE TYPE alt.status AS ENUM ('alt_open', 'alt_closed', 'alt_inactive')`,
		`CREATE TABLE foo (
a INT PRIMARY KEY, 
status status, 
alt alt.status, 
extra STRING,
FAMILY main (a, status, alt),
FAMILY extra (extra)
)`,
		`CREATE TABLE rowid (a INT)`, // This table has hidden rowid primary key column.
	)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	codec := execCfg.Codec
	fooDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	statusT := fooDesc.AllColumns()[1].GetType()
	altStatusT := fooDesc.AllColumns()[2].GetType()
	primarySpan := fooDesc.PrimaryIndexSpan(codec)
	pkEnd := primarySpan.EndKey
	fooID := fooDesc.GetID()

	ctx := context.Background()
	schemaTS := s.Clock().Now()
	eventDesc, err := newEventDescriptorForTarget(
		fooDesc, jobspb.ChangefeedTargetSpecification{}, schemaTS, false, false)
	require.NoError(t, err)
	extraFamDesc, err := newEventDescriptorForTarget(
		fooDesc, jobspb.ChangefeedTargetSpecification{
			Type:       jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
			FamilyName: "extra",
		}, schemaTS, false, false)
	require.NoError(t, err)

	rowidDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "rowid")
	rowidEventDesc, err := newEventDescriptorForTarget(
		rowidDesc, jobspb.ChangefeedTargetSpecification{}, schemaTS, false, false)
	require.NoError(t, err)

	rc := func(n string, typ *types.T) colinfo.ResultColumn {
		return colinfo.ResultColumn{Name: n, Typ: typ}
	}
	mainColumns := colinfo.ResultColumns{
		rc("a", types.Int), rc("status", statusT), rc("alt", altStatusT),
	}
	// mainColumns as tuple.
	mainColumnsTuple := colinfo.ResultColumns{
		rc("foo", types.MakeLabeledTuple(
			[]*types.T{types.Int, statusT, altStatusT},
			[]string{"a", "status", "alt"}),
		)}

	extraColumns := colinfo.ResultColumns{rc("a", types.Int), rc("extra", types.String)}
	// extraColumns as tuple.
	extraColumnsTuple := colinfo.ResultColumns{
		rc("foo", types.MakeLabeledTuple(
			[]*types.T{types.Int, types.String},
			[]string{"a", "extra"}),
		)}

	checkPresentation := func(t *testing.T, expected, found colinfo.ResultColumns) {
		t.Helper()
		require.Equal(t, len(expected), len(found), "e=%v f=%v", expected, found)
		for i := 0; i < len(found); i++ {
			require.Equal(t, expected[i].Name, found[i].Name, "e=%v f=%v", expected[i], found[i])
			require.True(t, expected[i].Typ.Equal(found[i].Typ), "e=%v f=%v", expected[i], found[i])
		}
	}

	for _, tc := range []struct {
		name         string
		desc         catalog.TableDescriptor
		stmt         string
		targetFamily string
		expectErr    string
		planSpans    roachpb.Spans
		presentation colinfo.ResultColumns
	}{
		{
			name:      "reject contradiction",
			desc:      fooDesc,
			stmt:      "SELECT * FROM foo WHERE a IS NULL",
			expectErr: `does not match any rows`,
		},
		{
			name:         "full table - main",
			desc:         fooDesc,
			stmt:         "SELECT * FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: mainColumns,
		},
		{
			name:         "full table - main tuple",
			desc:         fooDesc,
			stmt:         "SELECT foo FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: mainColumnsTuple,
		},
		{
			name:         "full table - double star",
			desc:         fooDesc,
			stmt:         "SELECT *, * FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: append(mainColumns, mainColumns...),
		},
		{
			name:         "full table extra family",
			desc:         fooDesc,
			targetFamily: "extra",
			stmt:         "SELECT * FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: extraColumns,
		},
		{
			name:         "full table extra family tuple",
			desc:         fooDesc,
			targetFamily: "extra",
			stmt:         "SELECT foo FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: extraColumnsTuple,
		},
		{
			name:         "expression scoped to column family",
			desc:         fooDesc,
			targetFamily: "extra",
			stmt:         "SELECT a, status, extra FROM foo",
			expectErr:    `column "status" does not exist`,
		},
		{
			name:         "full table extra family with cdc_prev tuple",
			desc:         fooDesc,
			targetFamily: "extra",
			stmt:         "SELECT *, cdc_prev FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: append(extraColumns, rc("cdc_prev", cdcPrevType(extraFamDesc))),
		},
		{
			name:         "full table with cdc_prev tuple",
			desc:         fooDesc,
			stmt:         "SELECT *, cdc_prev FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: append(mainColumns, rc("cdc_prev", cdcPrevType(eventDesc))),
		},
		{
			name:         "full table with cdc_prev expanded",
			desc:         fooDesc,
			stmt:         "SELECT *, (cdc_prev).* FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: append(mainColumns, mainColumns...),
		},
		{
			name:         "full table with cdc_prev json",
			desc:         fooDesc,
			stmt:         "SELECT *, row_to_json((cdc_prev).*) AS prev_json FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: append(mainColumns, rc("prev_json", types.Jsonb)),
		},
		{
			name:         "a > 10",
			desc:         fooDesc,
			stmt:         "SELECT * FROM foo WHERE a > 10",
			planSpans:    roachpb.Spans{{Key: mkPkKey(t, codec, fooID, 11), EndKey: pkEnd}},
			presentation: mainColumns,
		},
		{
			name:         "a > 10 with cdc_prev",
			desc:         fooDesc,
			stmt:         "SELECT * FROM foo WHERE a > 10 AND (cdc_prev).status = 'closed'",
			planSpans:    roachpb.Spans{{Key: mkPkKey(t, codec, fooID, 11), EndKey: pkEnd}},
			presentation: mainColumns,
		},
		{
			name:         "cast to standard type",
			desc:         fooDesc,
			stmt:         "SELECT 'cast'::string AS a, 'type_annotation':::string AS b FROM foo AS bar",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: colinfo.ResultColumns{rc("a", types.String), rc("b", types.String)},
		},
		{
			name:         "cast to table-typed tuple main",
			desc:         fooDesc,
			stmt:         "SELECT foo FROM foo",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: mainColumnsTuple,
		},
		{
			name:         "cast to table-typed tuple extra",
			desc:         fooDesc,
			stmt:         "SELECT foo FROM foo",
			targetFamily: "extra",
			planSpans:    roachpb.Spans{primarySpan},
			presentation: extraColumnsTuple,
		},
		{
			name:         "hidden columns hidden",
			desc:         rowidDesc,
			stmt:         "SELECT * FROM rowid",
			planSpans:    roachpb.Spans{rowidDesc.PrimaryIndexSpan(codec)},
			presentation: colinfo.ResultColumns{rc("a", types.Int)},
		},
		{
			name:      "hidden columns hidden with cdc_prev",
			desc:      rowidDesc,
			stmt:      "SELECT *, cdc_prev FROM rowid",
			planSpans: roachpb.Spans{rowidDesc.PrimaryIndexSpan(codec)},
			presentation: colinfo.ResultColumns{
				rc("a", types.Int),
				rc("cdc_prev", cdcPrevType(rowidEventDesc)),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseChangefeedExpression(tc.stmt)
			if err != nil {
				require.Regexp(t, tc.expectErr, err)
				return
			}
			target := jobspb.ChangefeedTargetSpecification{
				TableID:           tc.desc.GetID(),
				StatementTimeName: tc.desc.GetName(),
				FamilyName:        tc.targetFamily,
			}

			if tc.targetFamily != "" {
				target.Type = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
			}

			const splitFamilies = true
			_, _, plan, err := normalizeAndPlan(ctx, &execCfg, username.RootUserName(),
				defaultDBSessionData, tc.desc, schemaTS, target, sc, splitFamilies)

			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.planSpans, plan.Spans)
			checkPresentation(t, tc.presentation, plan.Presentation)
		})
	}
}

func mkPkKey(t *testing.T, codec keys.SQLCodec, tableID descpb.ID, vals ...int) roachpb.Key {
	t.Helper()

	// Encode index id, then each value.
	key, err := keyside.Encode(
		codec.TablePrefix(uint32(tableID)),
		tree.NewDInt(tree.DInt(1)), encoding.Ascending)

	require.NoError(t, err)
	for _, v := range vals {
		d := tree.NewDInt(tree.DInt(v))
		key, err = keyside.Encode(key, d, encoding.Ascending)
		require.NoError(t, err)
	}

	return key
}

// normalizeAndPlan normalizes select clause, and plans CDC expression execution
// (but does not execute).  Returns the plan along with normalized expression.
func normalizeAndPlan(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	sd *sessiondata.SessionData,
	descr catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target jobspb.ChangefeedTargetSpecification,
	sc *tree.SelectClause,
	splitFams bool,
) (norm *NormalizedSelectClause, withDiff bool, plan sql.CDCExpressionPlan, err error) {
	if err := withPlanner(ctx, execCfg, schemaTS, user, schemaTS, sd,
		func(ctx context.Context, execCtx sql.JobExecContext, cleanup func()) error {
			defer cleanup()

			norm, withDiff, err = NormalizeExpression(ctx, execCtx, descr, schemaTS, target, sc, splitFams)
			if err != nil {
				return err
			}

			// Add cdc_prev column; we may or may not need it, but we'll check below.
			prevCol, err := newPrevColumnForDesc(norm.desc)
			if err != nil {
				return err
			}
			plan, err = sql.PlanCDCExpression(ctx, execCtx,
				norm.SelectStatementForFamily(), sql.WithExtraColumn(prevCol))
			return err
		}); err != nil {
		return nil, false, sql.CDCExpressionPlan{}, err
	}
	return norm, withDiff, plan, nil
}
