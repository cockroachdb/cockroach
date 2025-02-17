// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/internal/rsg"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestFormatStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		stmt     string
		f        tree.FmtFlags
		expected string
	}{
		// This here checks encodeSQLString on non-tree.DString strings also
		// calls encodeSQLString with the right formatter.
		// See TestFormatExprs below for the test on DStrings.
		{`CREATE DATABASE foo TEMPLATE = 'bar-baz'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = bar-baz`},
		{`CREATE DATABASE foo TEMPLATE = 'bar baz'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar,baz'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar,baz'`},
		{`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`, tree.FmtBareStrings,
			`CREATE DATABASE foo TEMPLATE = 'bar{baz}'`},

		{`SET "time zone" = UTC`, tree.FmtSimple,
			`SET "time zone" = utc`},
		{`SET "time zone" = UTC`, tree.FmtBareIdentifiers,
			`SET time zone = utc`},
		{`SET "time zone" = UTC`, tree.FmtBareStrings,
			`SET "time zone" = utc`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			stmtStr := tree.AsStringWithFlags(stmt.AST, test.f)
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}

func TestFormatTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		stmt     string
		expected string
	}{
		{`CREATE TABLE foo (x INT8)`,
			`CREATE TABLE xoxoxo (x INT8)`},
		{`INSERT INTO foo(x) TABLE bar`,
			`INSERT INTO xoxoxo(x) TABLE xoxoxo`},
		{`UPDATE foo SET x = y`,
			`UPDATE xoxoxo SET x = y`},
		{`DELETE FROM foo`,
			`DELETE FROM xoxoxo`},
		{`ALTER TABLE foo RENAME TO bar`,
			`ALTER TABLE xoxoxo RENAME TO xoxoxo`},
		{`SHOW COLUMNS FROM foo`,
			`SHOW COLUMNS FROM xoxoxo`},
		{`SHOW CREATE TABLE foo`,
			`SHOW CREATE xoxoxo`},
		// TODO(knz): TRUNCATE and GRANT table names are removed by
		// tree.FmtAnonymize but not processed by table formatters.
		//
		// {`TRUNCATE foo`,
		// `TRUNCATE TABLE xoxoxo`},
		// {`GRANT SELECT ON bar TO foo`,
		// `GRANT SELECT ON xoxoxo TO foo`},
	}

	f := tree.NewFmtCtx(
		tree.FmtSimple,
		tree.FmtReformatTableNames(func(ctx *tree.FmtCtx, _ *tree.TableName) {
			ctx.WriteString("xoxoxo")
		}),
	)

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			f.Reset()
			f.FormatNode(stmt.AST)
			stmtStr := f.String()
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}

func TestFormatExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr     string
		f        tree.FmtFlags
		expected string
	}{
		{`null`, tree.FmtShowTypes,
			`(NULL)[unknown]`},
		{`true`, tree.FmtShowTypes,
			`(true)[bool]`},
		{`123`, tree.FmtShowTypes,
			`(123)[int]`},
		{`123.456`, tree.FmtShowTypes,
			`(123.456)[decimal]`},
		{`'abc'`, tree.FmtShowTypes,
			`('abc')[string]`},
		{`b'abc'`, tree.FmtShowTypes,
			`('\x616263')[bytes]`},
		{`B'10010'`, tree.FmtShowTypes,
			`(B'10010')[varbit]`},
		{`interval '3s'`, tree.FmtShowTypes,
			`('00:00:03')[interval]`},
		{`date '2003-01-01'`, tree.FmtShowTypes,
			`('2003-01-01')[date]`},
		{`date 'today'`, tree.FmtShowTypes,
			`(('today')[string]::DATE)[date]`},
		{`timestamp '2003-01-01 00:00:00'`, tree.FmtShowTypes,
			`('2003-01-01 00:00:00')[timestamp]`},
		{`timestamp 'now'`, tree.FmtShowTypes,
			`(('now')[string]::TIMESTAMP)[timestamp]`},
		{`timestamptz '2003-01-01 00:00:00+03:00'`, tree.FmtShowTypes,
			`('2003-01-01 00:00:00+03')[timestamptz]`},
		{`timestamptz '2003-01-01 00:00:00'`, tree.FmtShowTypes,
			`(('2003-01-01 00:00:00')[string]::TIMESTAMPTZ)[timestamptz]`},
		{`greatest(unique_rowid(), 12)`, tree.FmtShowTypes,
			`(greatest((unique_rowid())[int], (12)[int]))[int]`},

		// While TestFormatStmt above checks StrVals, this here
		// checks DStrings.
		{`ARRAY['a','b c','d,e','f{g','h}i']`, tree.FmtBareStrings,
			`ARRAY[a, 'b c', 'd,e', 'f{g', 'h}i']`},
		// TODO(jordan): pg does *not* quote strings merely containing hex
		// escapes when included in array values. #16487
		// {`ARRAY[e'j\x10k']`, tree.FmtBareStrings,
		//	 `ARRAY[j\x10k]`},

		{`(-1):::INT`, tree.FmtParsableNumerics, "(-1)"},
		{`'NaN':::FLOAT`, tree.FmtParsableNumerics, "'NaN'"},
		{`'-Infinity':::FLOAT`, tree.FmtParsableNumerics, "'-Inf'"},
		{`'Infinity':::FLOAT`, tree.FmtParsableNumerics, "'+Inf'"},
		{`3.00:::FLOAT`, tree.FmtParsableNumerics, "3.0"},
		{`(-3.00):::FLOAT`, tree.FmtParsableNumerics, "(-3.0)"},
		{`'NaN':::DECIMAL`, tree.FmtParsableNumerics, "'NaN'"},
		{`'-Infinity':::DECIMAL`, tree.FmtParsableNumerics, "'-Infinity'"},
		{`'Infinity':::DECIMAL`, tree.FmtParsableNumerics, "'Infinity'"},
		{`3.00:::DECIMAL`, tree.FmtParsableNumerics, "3.00"},
		{`(-3.00):::DECIMAL`, tree.FmtParsableNumerics, "(-3.00)"},

		{`1`, tree.FmtParsable, "1:::INT8"},
		{`1:::INT`, tree.FmtParsable, "1:::INT8"},
		{`9223372036854775807`, tree.FmtParsable, "9223372036854775807:::INT8"},
		{`9223372036854775808`, tree.FmtParsable, "9223372036854775808:::DECIMAL"},
		{`-1`, tree.FmtParsable, "(-1):::INT8"},
		{`(-1):::INT`, tree.FmtParsable, "(-1):::INT8"},
		{`-9223372036854775808`, tree.FmtParsable, "(-9223372036854775808):::INT8"},
		{`-9223372036854775809`, tree.FmtParsable, "(-9223372036854775809):::DECIMAL"},
		{`(-92233.1):::FLOAT`, tree.FmtParsable, "(-92233.1):::FLOAT8"},
		{`92233.00:::DECIMAL`, tree.FmtParsable, "92233.00:::DECIMAL"},

		{`B'00100'`, tree.FmtParsable, "B'00100'"},

		{`unique_rowid() + 123`, tree.FmtParsable,
			`unique_rowid() + 123:::INT8`},
		{`sqrt(123.0) + 456`, tree.FmtParsable,
			`sqrt(123.0:::DECIMAL) + 456:::DECIMAL`},
		{`ROW()`, tree.FmtParsable, `()`},
		{`now() + interval '3s'`, tree.FmtSimple,
			`now() + '00:00:03'`},
		{`now() + interval '3s'`, tree.FmtParsable,
			`now():::TIMESTAMPTZ + '00:00:03':::INTERVAL`},
		{`current_date() - date '2003-01-01'`, tree.FmtSimple,
			`current_date() - '2003-01-01'`},
		{`current_date() - date '2003-01-01'`, tree.FmtParsable,
			`current_date() - '2003-01-01':::DATE`},
		{`current_date() - date 'yesterday'`, tree.FmtSimple,
			`current_date() - 'yesterday'::DATE`},
		{`now() - timestamp '2003-01-01'`, tree.FmtSimple,
			`now() - '2003-01-01 00:00:00'`},
		{`now() - timestamp '2003-01-01'`, tree.FmtParsable,
			`now():::TIMESTAMPTZ - '2003-01-01 00:00:00':::TIMESTAMP`},
		{`'+Inf':::DECIMAL + '-Inf':::DECIMAL + 'NaN':::DECIMAL`, tree.FmtParsable,
			`('Infinity':::DECIMAL + '-Infinity':::DECIMAL) + 'NaN':::DECIMAL`},
		{`'+Inf':::FLOAT8 + '-Inf':::FLOAT8 + 'NaN':::FLOAT8`, tree.FmtParsable,
			`('+Inf':::FLOAT8 + '-Inf':::FLOAT8) + 'NaN':::FLOAT8`},
		{`'12:00:00':::TIME`, tree.FmtParsable, `'12:00:00':::TIME`},
		{`'63616665-6630-3064-6465-616462656562':::UUID`, tree.FmtParsable,
			`'63616665-6630-3064-6465-616462656562':::UUID`},

		{`123::INT`, tree.FmtCheckEquivalence, `123:::INT8`},
		{`ARRAY[1, 2]::INT[]`, tree.FmtCheckEquivalence, `ARRAY[1:::INT8, 2:::INT8]:::INT8[]`},
		{`(123:::INT, 123:::DECIMAL)`, tree.FmtCheckEquivalence,
			`(123:::INT8, 123:::DECIMAL)`},

		{`(1, COALESCE(NULL, 123), ARRAY[45.6])`, tree.FmtHideConstants,
			`(_, COALESCE(_, _), ARRAY[_])`},
	}

	ctx := context.Background()
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			semaContext := tree.MakeSemaContext(nil /* resolver */)
			typeChecked, err := tree.TypeCheck(ctx, expr, &semaContext, types.AnyElement)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(typeChecked, test.f)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}

// TestFormatUntypedExpr is similar to TestFormatExpr, but it does not
// type-check the test expressions before formatting them. It allows for testing
// formatting of mis-typed expressions.
func TestFormatUntypedExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr     string
		f        tree.FmtFlags
		expected string
	}{
		{"((1, 2) AS foo)", tree.FmtHideConstants, "((_, _) AS foo)"},
		{"((1, 2, 3) AS foo)", tree.FmtHideConstants, "((_, _, __more1_10__) AS foo)"},
		{"((1, 2, 3) AS foo, bar)", tree.FmtHideConstants, "((_, _, __more1_10__) AS foo, bar)"},
		{"((1, 2, 3) AS foo, bar, baz)", tree.FmtHideConstants, "((_, _, __more1_10__) AS foo, bar)"},
		{"((1, 2) AS foo, bar)", tree.FmtHideConstants, "((_, _) AS foo, bar)"},
		{"((1, 2) AS foo, bar, baz)", tree.FmtHideConstants, "((_, _) AS foo, bar, baz)"},
		{"(ROW(1) AS foo)", tree.FmtHideConstants, "((_,) AS foo)"},
		{"(ROW(1) AS foo, bar)", tree.FmtHideConstants, "((_,) AS foo, bar)"},
		{"(ROW(1) AS foo, bar, baz)", tree.FmtHideConstants, "((_,) AS foo, bar, baz)"},
		{"(ROW(1, 2) AS foo, bar, baz)", tree.FmtHideConstants, "((_, _) AS foo, bar, baz)"},
		{"(ROW(1, 2, 3) AS foo, bar)", tree.FmtHideConstants, "((_, _, __more1_10__) AS foo, bar)"},
		{"(ROW(1, 2, 3) AS foo, bar, baz)", tree.FmtHideConstants, "((_, _, __more1_10__) AS foo, bar)"},
		{"(ROW(1, 2, 3) AS foo)", tree.FmtHideConstants, "((_, _, __more1_10__) AS foo)"},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(expr, test.f)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}

func TestFmtShortenConstants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr     string
		expected string
	}{
		{`VALUES (1), (2)`, `VALUES (1), (2)`},
		{`VALUES (1), (2), (3), (4)`, `VALUES (1), (2), (__more1_10__), (4)`},
		{`VALUES (1), (2), (3), (4), (5), (6)`, `VALUES (1), (2), (__more1_10__), (6)`},
		{`VALUES (ARRAY[1, 2])`, `VALUES (ARRAY[1, 2])`},
		{`VALUES (ARRAY[1, 2, 3, 4])`, `VALUES (ARRAY[1, 2, __more1_10__, 4])`},
		{`VALUES (ARRAY[1, 2, 3, 4, 5, 6])`, `VALUES (ARRAY[1, 2, __more1_10__, 6])`},
		{`SELECT (1, 2)`, `SELECT (1, 2)`},
		{`SELECT (1, 2, 3, 4)`, `SELECT (1, 2, __more1_10__, 4)`},
		{`SELECT (1, 2, 3, 4, 5, 6)`, `SELECT (1, 2, __more1_10__, 6)`},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			stmtStr := tree.AsStringWithFlags(stmt.AST, tree.FmtShortenConstants)
			if stmtStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, stmtStr)
			}
		})
	}
}

func TestFormatExpr2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	enumMembers := []string{"hi", "hello"}
	enumType := types.MakeEnum(catid.TypeIDToOID(500), catid.TypeIDToOID(100500))
	enumType.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Schema: "test",
			Name:   "greeting",
		},
		EnumData: &types.EnumMetadata{
			LogicalRepresentations: enumMembers,
			// The physical representations don't matter in this case, but the
			// enum related code in tree expects that the length of
			// PhysicalRepresentations is equal to the length of
			// LogicalRepresentations.
			PhysicalRepresentations: [][]byte{
				{0x42, 0x1},
				{0x42},
			},
			IsMemberReadOnly: make([]bool, len(enumMembers)),
		},
	}
	enumHi, err := tree.MakeDEnumFromLogicalRepresentation(enumType, enumMembers[0])
	if err != nil {
		t.Fatal(err)
	}
	enumHello, err := tree.MakeDEnumFromLogicalRepresentation(enumType, enumMembers[1])
	if err != nil {
		t.Fatal(err)
	}

	// This tests formatting from an stmt AST. Suitable for use if your input
	// isn't easily creatable from a string without running an Eval.
	testData := []struct {
		expr     tree.Expr
		f        tree.FmtFlags
		expected string
	}{
		{tree.NewDOidWithTypeAndName(10, types.RegClass, "foo"),
			tree.FmtParsable, `crdb_internal.create_regclass(10,'foo'):::REGCLASS`},
		{tree.NewDOidWithTypeAndName(10, types.RegNamespace, "foo"),
			tree.FmtParsable, `crdb_internal.create_regnamespace(10,'foo'):::REGNAMESPACE`},
		{tree.NewDOidWithTypeAndName(10, types.RegProc, "foo"),
			tree.FmtParsable, `crdb_internal.create_regproc(10,'foo'):::REGPROC`},
		{tree.NewDOidWithTypeAndName(10, types.RegProcedure, "foo"),
			tree.FmtParsable, `crdb_internal.create_regprocedure(10,'foo'):::REGPROCEDURE`},
		{tree.NewDOidWithTypeAndName(10, types.RegRole, "foo"),
			tree.FmtParsable, `crdb_internal.create_regrole(10,'foo'):::REGROLE`},
		{tree.NewDOidWithTypeAndName(10, types.RegType, "foo"),
			tree.FmtParsable, `crdb_internal.create_regtype(10,'foo'):::REGTYPE`},

		// Ensure that nulls get properly type annotated when printed in an
		// enclosing tuple that has a type for their position within the tuple.
		{tree.NewDTuple(
			types.MakeTuple([]*types.T{types.Int, types.String}),
			tree.DNull, tree.NewDString("foo")),
			tree.FmtParsable,
			`(NULL:::INT8, 'foo':::STRING)`,
		},
		{tree.NewDTuple(
			types.MakeTuple([]*types.T{types.Unknown, types.String}),
			tree.DNull, tree.NewDString("foo")),
			tree.FmtParsable,
			`(NULL, 'foo':::STRING)`,
		},
		{&tree.DArray{
			ParamTyp: types.Int,
			Array:    tree.Datums{tree.DNull, tree.DNull},
			HasNulls: true,
		},
			tree.FmtParsable,
			`ARRAY[NULL,NULL]:::INT8[]`,
		},
		{tree.NewDTuple(
			types.MakeTuple([]*types.T{enumType, enumType}),
			tree.DNull, &enumHi),
			tree.FmtParsable,
			`(NULL:::greeting, 'hi':::greeting)`,
		},

		// Ensure that enums get properly type annotated when printed in an
		// enclosing tuple for serialization purposes.
		{tree.NewDTuple(
			types.MakeTuple([]*types.T{enumType, enumType}),
			&enumHi, &enumHello),
			tree.FmtSerializable,
			`(x'4201':::@100500, x'42':::@100500)`,
		},
		{tree.NewDTuple(
			types.MakeTuple([]*types.T{enumType, enumType}),
			tree.DNull, &enumHi),
			tree.FmtSerializable,
			`(NULL:::@100500, x'4201':::@100500)`,
		},
	}

	ctx := context.Background()
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typeChecked, err := tree.TypeCheck(ctx, test.expr, &semaCtx, types.AnyElement)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(typeChecked, test.f)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}

func TestFormatPgwireText(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr     string
		expected string
	}{
		{`true`, `t`},
		{`false`, `f`},
		{`ROW(1)`, `(1)`},
		{`ROW(1, NULL)`, `(1,)`},
		{`ROW(1, true, 3)`, `(1,t,3)`},
		{`ROW(1, (2, 3))`, `(1,"(2,3)")`},
		{`ROW(1, (2, 'a b'))`, `(1,"(2,""a b"")")`},
		{`ROW(1, (2, 'a"b'))`, `(1,"(2,""a""""b"")")`},
		{`ROW(1, 2, ARRAY[1,2,3])`, `(1,2,"{1,2,3}")`},
		{`ROW(1, 2, ARRAY[1,NULL,3])`, `(1,2,"{1,NULL,3}")`},
		{`ROW(1, 2, ARRAY['a','b','c'])`, `(1,2,"{a,b,c}")`},
		{`ROW(1, 2, ARRAY[true,false,true])`, `(1,2,"{t,f,t}")`},
		{`ARRAY[(1,2),(3,4)]`, `{"(1,2)","(3,4)"}`},
		{`ARRAY[(false,'a'),(true,'b')]`, `{"(f,a)","(t,b)"}`},
		{`ARRAY[(1,ARRAY[2,NULL])]`, `{"(1,\"{2,NULL}\")"}`},
		{`ARRAY[(1,(1,2)),(2,(3,4))]`, `{"(1,\"(1,2)\")","(2,\"(3,4)\")"}`},

		{`(((1, 'a b', 3), (4, 'c d'), ROW(6)), (7, 8), ROW('e f'))`,
			`("(""(1,""""a b"""",3)"",""(4,""""c d"""")"",""(6)"")","(7,8)","(""e f"")")`},

		{`(((1, '2', 3), (4, '5'), ROW(6)), (7, 8), ROW('9'))`,
			`("(""(1,2,3)"",""(4,5)"",""(6)"")","(7,8)","(9)")`},

		{`ARRAY[('a b',ARRAY['c d','e f']), ('g h',ARRAY['i j','k l'])]`,
			`{"(\"a b\",\"{\"\"c d\"\",\"\"e f\"\"}\")","(\"g h\",\"{\"\"i j\"\",\"\"k l\"\"}\")"}`},

		{`ARRAY[('1',ARRAY['2','3']), ('4',ARRAY['5','6'])]`,
			`{"(1,\"{2,3}\")","(4,\"{5,6}\")"}`},

		{`ARRAY[e'\U00002001☃']`, `{ ☃}`},
	}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typeChecked, err := tree.TypeCheck(ctx, expr, &semaCtx, types.AnyElement)
			if err != nil {
				t.Fatal(err)
			}
			typeChecked, err = normalize.Expr(ctx, evalCtx, typeChecked)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(typeChecked, tree.FmtPgwireText)
			if exprStr != test.expected {
				t.Fatalf("expected %s, got %s", test.expected, exprStr)
			}
		})
	}
}

func TestFormatNodeSummary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     `SELECT (SELECT count(*) FROM system.jobs) AS j, num_running, s.* FROM system.scheduled_jobs AS s WHERE next_run < current_timestamp() ORDER BY random() LIMIT 10 FOR UPDATE`,
			expected: `SELECT (SELECT FROM sy...)... FROM system.scheduled_jobs AS s`,
		},
		{
			stmt:     `SELECT status AS s, app_name FROM system.apps, system.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp() WHERE crdb_internal_aggregated_ts_app_name_fingerprint_id_node_id_shard_8 = $1`,
			expected: `SELECT status AS s, ap... FROM system.apps, system.transactio...`,
		},
		{
			stmt:     `SELECT app_name, aggregated_ts, fingerprint_id, metadata, statistics FROM system.app_statistics JOIN system.transaction_statistics ON crdb_internal.transaction_statistics.app_name = system.transaction_statistics.app_name`,
			expected: `SELECT app_name, aggre... FROM system.app_statistics JOIN sys...`,
		},
		{
			stmt:     `SELECT id FROM system.jobs, (SELECT $3::TIMESTAMP AS ts, $4::FLOAT8 AS initial_delay, $5::FLOAT8 AS max_delay) AS args WHERE ((status IN ('_', '_')) AND ((claim_session_id = $1) AND (claim_instance_id = $2))) AND (args.ts >= (COALESCE(last_run, created) + least(IF((args.initial_delay * (power(_, least(_, COALESCE(num_runs, _))) - _)::FLOAT8) >= _, args.initial_delay * (power(_, least(_, COALESCE(num_runs, _))) - _)::FLOAT8, args.max_delay), args.max_delay)::INTERVAL))`,
			expected: `SELECT id FROM system.jobs, (SELECT) AS args`,
		},
		{
			stmt:     `INSERT INTO system.public.lease("descID", version, "nodeID", expiration) VALUES ('1232', '111', __more1_10__)`,
			expected: `INSERT INTO system.public.lease("descID", versi...)`,
		},
		{
			stmt:     `INSERT INTO vehicles VALUES ($1, $2, __more1_10__)`,
			expected: `INSERT INTO vehicles`,
		},
		{
			stmt:     `UPSERT INTO system.reports_meta(id, "generated") VALUES ($1, $2)`,
			expected: `UPSERT INTO system.reports_meta(id, "generated")`,
		},
		{
			stmt:     `INSERT INTO system.table_statistics("tableID", name) SELECT 'cockroach', app_names FROM system.apps`,
			expected: `INSERT INTO system.table_statistics SELECT '_', app_names FROM system.apps`,
		},
		{
			stmt:     `INSERT INTO system.settings(name, value, "lastUpdated", "valueType") SELECT 'unique_pear', value, "lastUpdated", "valueType" FROM system.settings JOIN system.internal_tables ON system.settings.id = system.internal_tables.id WHERE name = '_' ON CONFLICT (name) DO NOTHING`,
			expected: `INSERT INTO system.settings SELECT '_', value, "la... FROM system.settings JOIN system.in...`,
		},
		{
			stmt:     `UPDATE system.jobs SET progress = 'value' WHERE id = '12312'`,
			expected: `UPDATE system.jobs SET progress = '_' WHERE id = '_'`,
		},
		{
			stmt:     `UPDATE system.jobs SET status = $2, payload = $3, last_run = $4, num_runs = $5 WHERE internal_table_id = $1`,
			expected: `UPDATE system.jobs SET status = $1, pa... WHERE internal_table_...`,
		},
		{
			stmt:     `UPDATE system.extra_extra_long_table_name SET (schedule_state, next_run) = ($1, $2) WHERE schedule_id = 'name'`,
			expected: `UPDATE system.extra_extra_long_table_... SET (schedule_state...)... WHERE schedule_id = '...`,
		},
		{
			stmt:     `SELECT (SELECT job_id FROM (SELECT * FROM system.jobs)) AS j, name, app_name FROM system.apps`,
			expected: `SELECT (SELECT FROM (S...))... FROM system.apps`,
		},
		{
			stmt:     `SELECT (SELECT job_id FROM system.jobs) FROM system.apps`,
			expected: `SELECT (SELECT FROM sy...) FROM system.apps`,
		},
		{
			stmt:     `SELECT (SELECT job_id FROM (SELECT * FROM system.jobs) AS c) AS j, name, app_name FROM system.apps`,
			expected: `SELECT (SELECT FROM (S...)...)... FROM system.apps`,
		},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			fmtFlags := tree.FmtSummary | tree.FmtHideConstants
			exprStr := tree.AsStringWithFlags(stmt.AST, fmtFlags)
			if exprStr != test.expected {
				t.Fatalf("expected %s, got %s", test.expected, exprStr)
			}
		})
	}
}

// BenchmarkFormatRandomStatements measures the time needed to format
// 1000 random statements.
func BenchmarkFormatRandomStatements(b *testing.B) {
	// Generate a bunch of random statements.
	var runfile string
	if bazel.BuiltWithBazel() {
		var err error
		runfile, err = bazel.Runfile("pkg/sql/parser/sql.y")
		if err != nil {
			panic(err)
		}
	} else {
		runfile = filepath.Join("..", "..", "parser", "sql.y")
	}
	yBytes, err := os.ReadFile(runfile)
	if err != nil {
		b.Fatalf("error reading grammar: %v", err)
	}
	// Use a constant seed so multiple runs are consistent.
	const seed = 1134
	r, err := rsg.NewRSG(seed, string(yBytes), false)
	if err != nil {
		b.Fatalf("error instantiating RSG: %v", err)
	}
	strs := make([]string, 1000)
	stmts := make([]tree.Statement, 1000)
	for i := 0; i < 1000; {
		rdm := r.Generate("stmt", 20)
		stmt, err := parser.ParseOne(rdm)
		if err != nil {
			// Some statements (e.g. those containing error terminals) do
			// not parse.  It's all right. Just ignore this and continue
			// until we have all we want.
			continue
		}
		strs[i] = rdm
		stmts[i] = stmt.AST
		i++
	}

	// Benchmark the parses.
	b.Run("parse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, sql := range strs {
				_, err := parser.ParseOne(sql)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	// Benchmark the formats.
	b.Run("format", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for i, stmt := range stmts {
				f := tree.NewFmtCtx(tree.FmtSimple)
				f.FormatNode(stmt)
				strs[i] = f.CloseAndGetString()
			}
		}
	})
}

// Verify FmtCollapseLists format flag works as expected.
func TestFmtCollapseListsFormatFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     `VALUES (())`,
			expected: `VALUES (())`,
		},
		{
			stmt:     `VALUES (1)`,
			expected: `VALUES (1)`,
		},
		{
			stmt:     `VALUES ((1, 2), (3, 4)), ((5, 6), (7, 8))`,
			expected: `VALUES ((1, __more__), __more__), (__more__)`,
		},
		{
			// For VALUES clauses, we always shorten the list of values.
			// The first expression will be shortened if possible, and the
			// rest of the values are replaced with __more__ regardless of
			// whether or not they contain names.
			stmt:     `VALUES ((a, b), (3, 4)), ((a, b), (1, 2))`,
			expected: `VALUES ((a, b), (3, __more__)), (__more__)`,
		},
		{
			stmt:     `SELECT * FROM foo WHERE f IN ()`,
			expected: `SELECT * FROM foo WHERE f IN ()`,
		},
		{
			stmt:     `SELECT * FROM foo WHERE f IN ($1)`,
			expected: `SELECT * FROM foo WHERE f IN ($1,)`,
		},
		{
			stmt:     `SELECT * FROM foo WHERE f IN (2, (3 - $1)::INT, 2*3, -$2, NULL, 'ok', (3,$3))`,
			expected: `SELECT * FROM foo WHERE f IN (2, __more__)`,
		},
		{
			stmt:     `SELECT * FROM foo WHERE f IN (1, 2, a, b, c)`,
			expected: `SELECT * FROM foo WHERE f IN (1, 2, a, b, c)`,
		},
		{
			stmt:     `SELECT * FROM foo WHERE f IN (1, 2, a + 1)`,
			expected: `SELECT * FROM foo WHERE f IN (1, 2, a + 1)`,
		},
		{
			stmt:     `SELECT ARRAY[]`,
			expected: `SELECT ARRAY[]`,
		},
		{
			stmt:     `SELECT ARRAY['crdb', 'world', CAST((2*2*3) AS FLOAT)]`,
			expected: `SELECT ARRAY['crdb', __more__]`,
		},
		{
			stmt:     `SELECT ARRAY[1+2, -2, $1::INT, NULL, 4*$2::INT, 'hello-world']`,
			expected: `SELECT ARRAY[1 + 2, __more__]`,
		},
		{
			stmt:     `SELECT ARRAY[1, 2, 3]`,
			expected: `SELECT ARRAY[1, __more__]`,
		},
		{
			stmt:     `SELECT ARRAY[1, 2, 3, a]`,
			expected: `SELECT ARRAY[1, 2, 3, a]`,
		},
		{
			stmt:     `SELECT * FROM foo WHERE f IN (1) AND f IN (2)`,
			expected: `SELECT * FROM foo WHERE (f IN (1,)) AND (f IN (2,))`,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d %s", i, test.stmt), func(t *testing.T) {
			stmt, err := parser.ParseOne(test.stmt)
			if err != nil {
				t.Fatal(err)
			}
			exprStr := tree.AsStringWithFlags(stmt.AST, tree.FmtCollapseLists)
			if exprStr != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, exprStr)
			}
		})
	}
}

func TestFormatStringDollarQuotes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		input string
		delim string
	}{
		{
			input: ``,
			delim: `$$`,
		},
		{
			input: `foo`,
			delim: `$$`,
		},
		{
			input: `foo $ bar`,
			delim: `$$`,
		},
		{
			input: `foo $bar$ baz`,
			delim: `$$`,
		},
		{
			input: `foo $$ bar`,
			delim: `$funcbody$`,
		},
		{
			input: `foo $$ $funcbody$ bar`,
			delim: `$funcbodyx$`,
		},
		{
			input: `foo $$ $funcbody$ $funcbodyx$ bar`,
			delim: `$funcbodyxx$`,
		},
		{
			input: `foo $funcbody$ bar`,
			delim: `$$`,
		},
		{
			input: `foo $$ $funcbodyx$ bar`,
			delim: `$funcbody$`,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d %s", i, test.input), func(t *testing.T) {
			f := tree.NewFmtCtx(tree.FmtSimple)
			f.FormatStringDollarQuotes(test.input)
			res := f.CloseAndGetString()
			expected := test.delim + test.input + test.delim
			if res != expected {
				t.Fatalf("expected %q, got %q", expected, res)
			}
		})
	}
}
