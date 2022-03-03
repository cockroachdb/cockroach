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
CREATE TABLE t.ds (it INTERVAL, s STRING, vc VARCHAR, c CHAR, t TIMESTAMP, n NAME, d DATE);
`); err != nil {
		t.Fatal(err)
	}

	desc := desctestutils.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "public", "ds")
	tDesc := desc.TableDesc()

	tests := []struct {
		expr   string
		expect string
	}{
		{
			expr:   "s::INTERVAL",
			expect: "parse_interval(s)::INTERVAL",
		},
		{
			expr:   "s::INTERVAL(4)",
			expect: "parse_interval(s)::INTERVAL(4)",
		},
		{
			expr:   "vc::DATE",
			expect: "parse_date(vc)::DATE",
		},
		{
			expr:   "n::TIME",
			expect: "parse_time(n)::TIME",
		},
		{
			expr:   "n::TIME(5)",
			expect: "parse_time(n)::TIME(5)",
		},
		{
			expr:   "parse_interval(s)",
			expect: "parse_interval(s)",
		},
		{
			expr:   "s::INT",
			expect: "s::INT8",
		},
		{
			expr:   "it::TEXT",
			expect: "to_char(it)::STRING",
		},
		{
			expr:   "vc::TIMETZ",
			expect: "parse_timetz(vc)::TIMETZ",
		},
		{
			expr:   "t::TIME",
			expect: "t::TIME",
		},
		{
			expr:   "s::TIME",
			expect: "parse_time(s)::TIME",
		},
		{
			expr:   `it::STRING = 'abc'`,
			expect: `to_char(it)::STRING = 'abc'`,
		},
		{
			expr:   "lower(it::STRING)",
			expect: "lower(to_char(it)::STRING)",
		},
		{
			expr:   "s::TIMESTAMPTZ::STRING",
			expect: "to_char(s::TIMESTAMPTZ)::STRING",
		},
		{
			expr:   "extract(epoch from s::TIME)",
			expect: "extract('epoch', parse_time(s)::TIME)",
		},
		{
			expr:   "extract(epoch from s::DATE)",
			expect: "extract('epoch', parse_date(s)::DATE)",
		},
		//Expected failures
		{
			expr:   "('foo' + 1)::STRING",
			expect: "unsupported binary operator: <string> + <int>",
		},
		{
			expr:   "t::BOOL",
			expect: "invalid cast: timestamp -> bool",
		},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			semaCtx.IntervalStyleEnabled = true
			semaCtx.DateStyleEnabled = true
			expr, err := parser.ParseExpr(test.expr)
			require.NoError(t, err)
			newExpr, _, err := sql.ResolveCastForStyleUsingVisitor(
				ctx,
				&semaCtx,
				tDesc,
				expr,
				tree.NewUnqualifiedTableName(tree.Name(desc.GetName())),
			)
			if err != nil {
				require.Equal(t, test.expect, err.Error())
			} else {
				require.Equal(t, test.expect, newExpr.String())
			}
		})
	}
}
