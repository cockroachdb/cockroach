// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestSupportedStatements(t *testing.T) {
	// Some statements n supportedStatements have multiple statementTags. This
	// map contains concrete statements that will return each tag, corresponding
	// one-to-one with each tag in statementTags.
	var multiTagStmts = map[reflect.Type][]tree.Statement{
		reflect.TypeOf((*tree.DropRoutine)(nil)):   {&tree.DropRoutine{}, &tree.DropRoutine{Procedure: true}},
		reflect.TypeOf((*tree.CreateRoutine)(nil)): {&tree.CreateRoutine{}, &tree.CreateRoutine{IsProcedure: true}},
	}

	sv := &settings.Values{}
	// Non-existent tags should error out.
	require.Error(t, forceDeclarativeStatements.Validate(sv, "FAKE STATEMENT"))
	// Generate the full set of statements
	allTags := strings.Builder{}
	noTags := strings.Builder{}
	first := true
	for typ, stmt := range supportedStatements {
		for i, tag := range stmt.statementTags {
			require.Greaterf(t, len(stmt.statementTags), 0, "statement tag is missing %v %v", typ, stmt)

			// Validate tags matches the statement tag.
			var expectedTag string
			if concreteStmts, ok := multiTagStmts[typ]; ok {
				expectedTag = concreteStmts[i].StatementTag()
			} else {
				// Otherwise, build a zero-value statement.
				typTag, found := typ.MethodByName("StatementTag")
				require.True(t, found, "unable to find stmt: %v %v", typ, stmt)
				ret := typTag.Func.Call([]reflect.Value{reflect.New(typ.Elem())})
				expectedTag = ret[0].String()
			}
			require.Equal(t, expectedTag, tag, "statement tag is different in AST")

			// Validate all tags are supported.
			require.NoError(t, forceDeclarativeStatements.Validate(sv, "+"+tag))
			require.NoError(t, forceDeclarativeStatements.Validate(sv, "!"+tag))
			// Validate all of them can be specified at once.
			if !first {
				allTags.WriteString(",")
				noTags.WriteString(",")
			}
			first = false
			allTags.WriteString("+")
			allTags.WriteString(tag)
			noTags.WriteString("!")
			noTags.WriteString(tag)
		}
	}
	require.NoError(t, forceDeclarativeStatements.Validate(sv, allTags.String()))
	require.NoError(t, forceDeclarativeStatements.Validate(sv, noTags.String()))
}
