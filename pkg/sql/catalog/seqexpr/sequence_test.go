// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package seqexpr

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestGetSequenceFromFunc(t *testing.T) {
	testData := []struct {
		expr     string
		expected *SeqIdentifier
	}{
		{`nextval('seq')`, &SeqIdentifier{SeqName: "seq"}},
		{`nextval(123::REGCLASS)`, &SeqIdentifier{SeqID: 123}},
		{`nextval(123)`, &SeqIdentifier{SeqID: 123}},
		{`nextval(123::OID::REGCLASS)`, &SeqIdentifier{SeqID: 123}},
		{`nextval(123::OID)`, &SeqIdentifier{SeqID: 123}},
	}

	ctx := context.Background()
	for i, test := range testData {
		t.Run(fmt.Sprintf("%d %s", i, test.expr), func(t *testing.T) {
			parsedExpr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			semaCtx := tree.MakeSemaContext()
			typedExpr, err := tree.TypeCheck(ctx, parsedExpr, &semaCtx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			funcExpr, ok := typedExpr.(*tree.FuncExpr)
			if !ok {
				t.Fatal("Expr is not a FuncExpr")
			}
			identifier, err := GetSequenceFromFunc(funcExpr)
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
		expected []SeqIdentifier
	}{
		{`nextval('seq')`, []SeqIdentifier{
			{SeqName: "seq"},
		}},
		{`nextval(123::REGCLASS)`, []SeqIdentifier{
			{SeqID: 123},
		}},
		{`nextval(123::REGCLASS) + nextval('seq')`, []SeqIdentifier{
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
			semaCtx := tree.MakeSemaContext()
			typedExpr, err := tree.TypeCheck(ctx, parsedExpr, &semaCtx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			identifiers, err := GetUsedSequences(typedExpr)
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
			semaCtx := tree.MakeSemaContext()
			typedExpr, err := tree.TypeCheck(ctx, parsedExpr, &semaCtx, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			newExpr, err := ReplaceSequenceNamesWithIDs(typedExpr, namesToID)
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
	t.Run("all seq references are by-ID (no upgrades)", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "public", "s1")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "public", "s2")
		tbl3 := tree.MakeTableNameWithSchema("", "sc1", "s3")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		usedSequenceIDsToNames[3] = &tbl3
		expr := "((nextval(1::REGCLASS) + nextval(2::REGCLASS)) + currval(3::REGCLASS)) + nextval(3::REGCLASS)"
		hasUpgraded, err := UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.False(t, hasUpgraded)
		require.Equal(t,
			"((nextval(1::REGCLASS) + nextval(2::REGCLASS)) + currval(3::REGCLASS)) + nextval(3::REGCLASS)",
			expr)
	})

	t.Run("all seq references are by-name", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "public", "s1")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "public", "s2")
		tbl3 := tree.MakeTableNameWithSchema("", "sc1", "s3")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		usedSequenceIDsToNames[3] = &tbl3
		expr := "nextval('public.s1') + nextval('testdb.public.s2') + currval('sc1.s3') + nextval('s3')"
		hasUpgraded, err := UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.True(t, hasUpgraded)
		require.Equal(t,
			"((nextval(1:::REGCLASS) + nextval(2:::REGCLASS)) + currval(3:::REGCLASS)) + nextval(3:::REGCLASS)",
			expr)
	})

	t.Run("mixed by-name and by-ID seq references", func(t *testing.T) {
		usedSequenceIDsToNames := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "public", "s1")
		tbl2 := tree.MakeTableNameWithSchema("testdb", "public", "s2")
		tbl3 := tree.MakeTableNameWithSchema("", "sc1", "s3")
		usedSequenceIDsToNames[1] = &tbl1
		usedSequenceIDsToNames[2] = &tbl2
		usedSequenceIDsToNames[3] = &tbl3
		expr := "nextval('public.s1') + nextval(2::REGCLASS) + currval('sc1.s3') + nextval('s3')"
		hasUpgraded, err := UpgradeSequenceReferenceInExpr(&expr, usedSequenceIDsToNames)
		require.NoError(t, err)
		require.True(t, hasUpgraded)
		require.Equal(t,
			"((nextval(1:::REGCLASS) + nextval(2::REGCLASS)) + currval(3:::REGCLASS)) + nextval(3:::REGCLASS)",
			expr)
	})
}

func TestSeqNameToIDMappingInExpr(t *testing.T) {
	t.Run("t1", func(t *testing.T) {
		// Test 1:
		// 		seqIDToNameMapping = {23 : 'db.sc1.t', 25 : 'db.sc2.t'}
		//		expr = "nextval('sc2.t')"
		//		expected return =  {"sc2.t" : 25} (because `db.sc2.t` best-matches `sc2.t`)
		seqIDToNameMapping := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("db", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("db", "sc2", "t")
		seqIDToNameMapping[23] = &tbl1
		seqIDToNameMapping[25] = &tbl2
		expr := "nextval('sc2.t')"
		seqNameToIDmappingInExpr, err := seqNameToIDMappingInExpr(expr, seqIDToNameMapping)
		require.NoError(t, err)
		require.Equal(t, 1, len(seqNameToIDmappingInExpr))
		require.Equal(t, descpb.ID(25), seqNameToIDmappingInExpr["sc2.t"])
	})

	t.Run("t2", func(t *testing.T) {
		// Test 2:
		// 		seqIDToNameMapping = {23 : 'db.sc1.t', 25 : 'sc2.t'}
		//		expr = "nextval('sc2.t')"
		//		expected return =  {"sc2.t" : 25} (because `sc2.t` best-matches `sc2.t`)
		seqIDToNameMapping := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("db", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
		seqIDToNameMapping[23] = &tbl1
		seqIDToNameMapping[25] = &tbl2
		expr := "nextval('sc2.t')"
		seqNameToIDmappingInExpr, err := seqNameToIDMappingInExpr(expr, seqIDToNameMapping)
		require.NoError(t, err)
		require.Equal(t, 1, len(seqNameToIDmappingInExpr))
		require.Equal(t, descpb.ID(25), seqNameToIDmappingInExpr["sc2.t"])
	})

	t.Run("t3", func(t *testing.T) {
		// Test 3:
		// 		seqIDToNameMapping = {23 : 'db.sc1.t', 25 : 'sc2.t'}
		//		expr = "nextval('db.sc2.t')"
		//		expected return =  {"db.sc2.t" : 25} (because `sc2.t` best-matches `db.sc2.t`)
		seqIDToNameMapping := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("db", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
		seqIDToNameMapping[23] = &tbl1
		seqIDToNameMapping[25] = &tbl2
		expr := "nextval('db.sc2.t')"
		seqNameToIDmappingInExpr, err := seqNameToIDMappingInExpr(expr, seqIDToNameMapping)
		require.NoError(t, err)
		require.Equal(t, 1, len(seqNameToIDmappingInExpr))
		require.Equal(t, descpb.ID(25), seqNameToIDmappingInExpr["db.sc2.t"])
	})

	t.Run("t4", func(t *testing.T) {
		// Test 4:
		// 		seqIDToNameMapping = {23 : 'sc1.t', 25 : 'sc2.t'}
		//		expr = "nextval('t')"
		//		expected return = non-nil error (because both 'sc1.t' and 'sc2.t' are equally good matches
		//				 		 					for 't' and we cannot decide)
		seqIDToNameMapping := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
		seqIDToNameMapping[23] = &tbl1
		seqIDToNameMapping[25] = &tbl2
		expr := "nextval('t')"
		_, err := seqNameToIDMappingInExpr(expr, seqIDToNameMapping)
		require.Errorf(t, err, "more than 1 matches found for t")
	})

	t.Run("t5", func(t *testing.T) {
		// Test 5:
		// 		seqIDToNameMapping = {23 : 'sc1.t', 25 : 'sc2.t'}
		//		expr = "nextval('t2')"
		//		expected return = non-nil error (because neither 'sc1.t' nor 'sc2.t' matches 't2')
		seqIDToNameMapping := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
		seqIDToNameMapping[23] = &tbl1
		seqIDToNameMapping[25] = &tbl2
		expr := "nextval('t2')"
		_, err := seqNameToIDMappingInExpr(expr, seqIDToNameMapping)
		require.Errorf(t, err, "no table name found to match input t2")
	})

	t.Run("t6", func(t *testing.T) {
		// Test 6:
		// 		seqIDToNameMapping = {23 : 'sc1.t', 25 : 'sc2.t', 27 : 't2'}
		//		expr = "nextval('sc1.t') + currval('testdb.sc2.t') + nextval('t2') + nextval('sc2.t2')"
		//		expected return = {'sc1.t' : 23, 'testdb.sc2.t' : 25, 't2' : 27, 'sc2.t2' : 27}
		seqIDToNameMapping := make(map[descpb.ID]*tree.TableName)
		tbl1 := tree.MakeTableNameWithSchema("", "sc1", "t")
		tbl2 := tree.MakeTableNameWithSchema("", "sc2", "t")
		tbl3 := tree.MakeTableNameWithSchema("", "", "t2")
		seqIDToNameMapping[23] = &tbl1
		seqIDToNameMapping[25] = &tbl2
		seqIDToNameMapping[27] = &tbl3
		expr := "nextval('sc1.t') + currval('testdb.sc2.t') + nextval('t2') + nextval('sc2.t2')"
		seqNameToIDmappingInExpr, err := seqNameToIDMappingInExpr(expr, seqIDToNameMapping)
		require.NoError(t, err)
		require.Equal(t, 4, len(seqNameToIDmappingInExpr))
		require.Equal(t, descpb.ID(23), seqNameToIDmappingInExpr["sc1.t"])
		require.Equal(t, descpb.ID(25), seqNameToIDmappingInExpr["testdb.sc2.t"])
		require.Equal(t, descpb.ID(27), seqNameToIDmappingInExpr["t2"])
		require.Equal(t, descpb.ID(27), seqNameToIDmappingInExpr["sc2.t2"])
	})
}
