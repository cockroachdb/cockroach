// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/stretchr/testify/require"
)

func TestSupportedStatements(t *testing.T) {
	sv := &settings.Values{}
	// Non-existent tags should error out.
	require.Error(t, forceDeclarativeStatements.Validate(sv, "FAKE STATEMENT"))
	// Generate the full set of statements
	allTags := strings.Builder{}
	noTags := strings.Builder{}
	first := true
	for typ, stmt := range supportedStatements {
		require.Greaterf(t, len(stmt.statementTag), 0, "statement tag is missing %v %v", typ, stmt)
		// Validate tags matches the statement tag
		typTag, found := typ.MethodByName("StatementTag")
		require.True(t, found, "unable to find stmt: %v %v", typ, stmt)
		ret := typTag.Func.Call([]reflect.Value{reflect.New(typ.Elem())})
		require.Equal(t, ret[0].String(), stmt.statementTag, "statement tag is different in AST")
		// Validate all tags are supported.
		require.NoError(t, forceDeclarativeStatements.Validate(sv, "+"+stmt.statementTag))
		require.NoError(t, forceDeclarativeStatements.Validate(sv, "!"+stmt.statementTag))
		// Validate all of them can be specified at once.
		if !first {
			allTags.WriteString(",")
			noTags.WriteString(",")
		}
		first = false
		allTags.WriteString("+")
		allTags.WriteString(stmt.statementTag)
		noTags.WriteString("!")
		noTags.WriteString(stmt.statementTag)
	}
	require.NoError(t, forceDeclarativeStatements.Validate(sv, allTags.String()))
	require.NoError(t, forceDeclarativeStatements.Validate(sv, noTags.String()))
}
