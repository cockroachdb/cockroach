// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestFixCastForStyleVisitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	ctx := context.Background()
	var semaCtx tree.SemaContext
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.ds (it INTERVAL, s STRING, vc VARCHAR, c CHAR, t TIMESTAMP, n NAME);
`); err != nil {
		t.Fatal(err)
	}

	var visitor sql.FixCastForStyleVisitor
	desc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "public", "ds")
	tDesc := desc.TableDesc()

	tests := []struct {
		expr   string
		typ    *types.T
		expect string
	}{
		{
			expr:   "s::INTERVAL",
			typ:    types.Interval,
			expect: "parse_interval(s)",
		},
		{
			expr:   "vc::INTERVAL",
			typ:    types.Interval,
			expect: "parse_interval(vc)",
		},
		{
			expr:   "n::INTERVAL",
			typ:    types.Interval,
			expect: "parse_interval(n)",
		},
		{
			expr:   "parse_interval(s)",
			typ:    types.Interval,
			expect: "parse_interval(s)",
		},
		{
			expr:   "s::INT",
			typ:    types.Int,
			expect: "s::INT8",
		},
		{
			expr:   "it::TEXT",
			typ:    types.String,
			expect: "to_char(it)",
		},
		{
			expr:   "t::TIME",
			typ:    types.Time,
			expect: "t::TIME",
		},
		{
			expr:   "t::TIMESTAMPTZ",
			typ:    types.TimestampTZ,
			expect: "t::TIMESTAMPTZ",
		},
		{
			expr:   "t::CHAR",
			typ:    types.String,
			expect: "to_char(t)",
		},
		{
			expr:   `it::STRING = 'abc'`,
			typ:    types.String,
			expect: `to_char(it) = 'abc'`,
		},
		{
			expr:   "lower(it::STRING)",
			typ:    types.String,
			expect: "lower(to_char(it))",
		},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			semaCtx.IntervalStyleEnabled = true
			semaCtx.DateStyleEnabled = true
			visitor = sql.MakeFixCastForStyleVisitor(ctx, &semaCtx, tDesc, test.typ, nil)
			expr, err := parser.ParseExpr(test.expr)
			require.NoError(t, err)
			newExpr, _ := tree.WalkExpr(&visitor, expr)
			require.Equal(t, test.expect, newExpr.String())
		})
	}
}
