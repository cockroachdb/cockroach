// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package seqexpr_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins" // register all builtins in builtins:init() for seqexpr package
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestGetSequenceFromFunc(t *testing.T) {
	testData := []struct {
		expr     string
		expected *seqexpr.SeqIdentifier
	}{
		{`nextval('seq')`, &seqexpr.SeqIdentifier{SeqName: "seq"}},
		{`nextval(123::REGCLASS)`, &seqexpr.SeqIdentifier{SeqID: 123}},
		{`nextval(123)`, &seqexpr.SeqIdentifier{SeqID: 123}},
		{`nextval(123::OID::REGCLASS)`, &seqexpr.SeqIdentifier{SeqID: 123}},
		{`nextval(123::OID)`, &seqexpr.SeqIdentifier{SeqID: 123}},
	}

	ctx := context.Background()
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			parsedExpr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typedExpr, err := tree.TypeCheck(ctx, parsedExpr, &semaCtx, types.AnyElement)
			if err != nil {
				t.Fatal(err)
			}
			funcExpr, ok := typedExpr.(*tree.FuncExpr)
			if !ok {
				t.Fatal("Expr is not a FuncExpr")
			}
			identifier, err := seqexpr.GetSequenceFromFunc(funcExpr)
			if err != nil {
				t.Fatal(err)
			}
			if identifier.IsByID() {
				if identifier.SeqID != test.expected.SeqID {
					t.Fatalf("expected %d, got %d", test.expected.SeqID, identifier.SeqID)
				}
			} else {
				if identifier.SeqName != test.expected.SeqName {
					t.Fatalf("expected %s, got %s", test.expected.SeqName, identifier.SeqName)
				}
			}
		})
	}
}

func TestGetUsedSequences(t *testing.T) {
	testData := []struct {
		expr     string
		expected []seqexpr.SeqIdentifier
	}{
		{`nextval('seq')`, []seqexpr.SeqIdentifier{
			{SeqName: "seq"},
		}},
		{`nextval(123::REGCLASS)`, []seqexpr.SeqIdentifier{
			{SeqID: 123},
		}},
		{`nextval(123::REGCLASS) + nextval('seq')`, []seqexpr.SeqIdentifier{
			{SeqID: 123},
			{SeqName: "seq"},
		}},
	}

	ctx := context.Background()
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			parsedExpr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typedExpr, err := tree.TypeCheck(ctx, parsedExpr, &semaCtx, types.AnyElement)
			if err != nil {
				t.Fatal(err)
			}
			identifiers, err := seqexpr.GetUsedSequences(typedExpr)
			if err != nil {
				t.Fatal(err)
			}

			if len(identifiers) != len(test.expected) {
				t.Fatalf("expected %d identifiers, got %d", len(test.expected), len(identifiers))
			}

			for i, identifier := range identifiers {
				if identifier.IsByID() {
					if identifier.SeqID != test.expected[i].SeqID {
						t.Fatalf("expected %d, got %d", test.expected[i].SeqID, identifier.SeqID)
					}
				} else {
					if identifier.SeqName != test.expected[i].SeqName {
						t.Fatalf("expected %s, got %s", test.expected[i].SeqName, identifier.SeqName)
					}
				}
			}
		})
	}
}

func TestReplaceSequenceNamesWithIDs(t *testing.T) {
	namesToID := map[string]descpb.ID{
		"seq": 123,
	}

	testData := []struct {
		expr     string
		expected string
	}{
		{`nextval('seq')`, `nextval(123:::REGCLASS)`},
		{`nextval('non_existent')`, `nextval('non_existent')`},
		{`nextval(123::REGCLASS)`, `nextval(123::REGCLASS)`},
		{`nextval(123)`, `nextval(123)`},
	}

	ctx := context.Background()
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			parsedExpr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typedExpr, err := tree.TypeCheck(ctx, parsedExpr, &semaCtx, types.AnyElement)
			if err != nil {
				t.Fatal(err)
			}
			newExpr, err := seqexpr.ReplaceSequenceNamesWithIDs(typedExpr, namesToID)
			if err != nil {
				t.Fatal(err)
			}
			if newExpr.String() != test.expected {
				t.Fatalf("expected %s, got %s", test.expected, newExpr.String())
			}
		})
	}
}

func TestUpgradeSequenceReferenceInExpr(t *testing.T) {
	t.Run("test name-matching -- fully resolved candidate names", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("testdb", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "sc2", "t")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		expr := "nextval('testdb.sc1.t') + nextval('sc1.t')"
		hasUpgraded, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.True(t, hasUpgraded)
		require.Equal(t,
			"nextval(1:::REGCLASS) + nextval(1:::REGCLASS)",
			expr)
	})

	t.Run("test name-matching -- partially resolved candidate names", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "sc2", "t")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		expr := "nextval('testdb.sc1.t') + nextval('sc1.t')"
		hasUpgraded, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.True(t, hasUpgraded)
		require.Equal(t,
			"nextval(1:::REGCLASS) + nextval(1:::REGCLASS)",
			expr)
	})

	t.Run("test name-matching -- public schema will be assumed when it's missing in candidate names",
		func(t *testing.T) {
			usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
			tbl1 := tree.MakeTableNameWithSchema("testdb", "", "t")
			tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
			usedSequenceIDsToNames[1] = &tbl1
			usedSequenceIDsToNames[2] = &tbl2
			expr := "nextval('testdb.public.t') + nextval('testdb.t')"
			hasUpgraded, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
			require.NoError(t, err)
			require.True(t, hasUpgraded)
			require.Equal(t,
				"nextval(1:::REGCLASS) + nextval(1:::REGCLASS)",
				expr)
		})

	t.Run("test name-matching -- ambiguous name matching, >1 candidates", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		expr := "nextval('t')"
		_, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.Error(t, err, "ambiguous name matching for 't'; both 'sc1.t' and 'sc2.t' match it.")
		require.Equal(t, "more than 1 matches found for \"t\"", err.Error())
	})

	t.Run("test name-matching -- no matching name, 0 candidate", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		expr := "nextval('t2')"
		_, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.Error(t, err, "no matching name for 't2'; neither 'sc1.t' nor 'sc2.t' match it.")
		require.Equal(t, "no table name found to match input \"t2\"", err.Error())
	})

	t.Run("all seq references are by-ID (no upgrades)", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("testdb", "public", "s1")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "public", "s2")
		tbl3 := tree.MakeTableNameWithSchema("testdb", "sc1", "s3")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		usedSequenceIDsToNames[3] = &tbl3
		expr := "((nextval(1::REGCLASS) + nextval(2::REGCLASS)) + currval(3::REGCLASS)) + nextval(3::REGCLASS)"
		hasUpgraded, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.False(t, hasUpgraded)
		require.Equal(t,
			"((nextval(1::REGCLASS) + nextval(2::REGCLASS)) + currval(3::REGCLASS)) + nextval(3::REGCLASS)",
			expr)
	})

	t.Run("all seq references are by-name", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("testdb", "public", "s1")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "public", "s2")
		tbl3 := tree.MakeTableNameWithSchema("testdb", "sc1", "s3")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		usedSequenceIDsToNames[3] = &tbl3
		expr := "nextval('testdb.public.s1') + nextval('testdb.public.s2') + currval('testdb.sc1.s3') + nextval('testdb.sc1.s3')"
		hasUpgraded, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.True(t, hasUpgraded)
		require.Equal(t,
			"((nextval(1:::REGCLASS) + nextval(2:::REGCLASS)) + currval(3:::REGCLASS)) + nextval(3:::REGCLASS)",
			expr)
	})

	t.Run("mixed by-name and by-ID seq references", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("testdb", "public", "s1")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "public", "s2")
		tbl3 := tree.MakeTableNameWithSchema("testdb", "sc1", "s3")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		usedSequenceIDsToNames[3] = &tbl3
		expr := "nextval('testdb.public.s1') + nextval(2::REGCLASS) + currval('testdb.sc1.s3') + nextval('testdb.sc1.s3')"
		hasUpgraded, err := seqexpr.UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.True(t, hasUpgraded)
		require.Equal(t,
			"((nextval(1:::REGCLASS) + nextval(2::REGCLASS)) + currval(3:::REGCLASS)) + nextval(3:::REGCLASS)",
			expr)
	})
}
