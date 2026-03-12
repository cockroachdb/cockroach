// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hintpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestHintType(t *testing.T) {
	// Test empty (unset) hint returns UNKNOWN.
	var emptyHint StatementHintUnion
	require.Equal(t, HintTypeEmpty, emptyHint.HintType())

	// Test InjectHints returns REWRITE INLINE HINTS.
	var injectHint StatementHintUnion
	injectHint.SetValue(&InjectHints{DonorSQL: "SELECT * FROM t"})
	require.Equal(t, HintTypeRewriteInlineHints, injectHint.HintType())

	// Test SessionVariableHint returns SET VARIABLE.
	var sessionHint StatementHintUnion
	sessionHint.SetValue(&SessionVariableHint{VariableName: "distsql", VariableValue: "on"})
	require.Equal(t, HintTypeSetVariable, sessionHint.HintType())
}

func TestRecreateStmt(t *testing.T) {
	tests := []struct {
		name     string
		hint     StatementHintUnion
		stmt     string
		database tree.Datum
		expected string
		ok       bool
	}{
		{
			name: "inject hints",
			hint: func() StatementHintUnion {
				var h StatementHintUnion
				h.SetValue(&InjectHints{DonorSQL: "SELECT * FROM t@idx"})
				return h
			}(),
			stmt:     "SELECT * FROM t",
			database: tree.DNull,
			expected: "SELECT information_schema.crdb_rewrite_inline_hints('SELECT * FROM t', 'SELECT * FROM t@idx');",
			ok:       true,
		},
		{
			name: "inject hints with database",
			hint: func() StatementHintUnion {
				var h StatementHintUnion
				h.SetValue(&InjectHints{DonorSQL: "SELECT * FROM t@idx"})
				return h
			}(),
			stmt:     "SELECT * FROM t",
			database: tree.NewDString("mydb"),
			expected: "SELECT information_schema.crdb_rewrite_inline_hints('SELECT * FROM t', 'SELECT * FROM t@idx', 'mydb');",
			ok:       true,
		},
		{
			name: "session variable basic",
			hint: func() StatementHintUnion {
				var h StatementHintUnion
				h.SetValue(&SessionVariableHint{VariableName: "distsql", VariableValue: "on"})
				return h
			}(),
			stmt:     "SELECT * FROM t",
			database: tree.DNull,
			expected: "SELECT information_schema.crdb_set_session_variable_hint('SELECT * FROM t', 'distsql', 'on');",
			ok:       true,
		},
		{
			name: "session variable with database",
			hint: func() StatementHintUnion {
				var h StatementHintUnion
				h.SetValue(&SessionVariableHint{VariableName: "distsql", VariableValue: "on"})
				return h
			}(),
			stmt:     "SELECT * FROM t",
			database: tree.NewDString("testdb"),
			expected: "SELECT information_schema.crdb_set_session_variable_hint('SELECT * FROM t', 'distsql', 'on', 'testdb');",
			ok:       true,
		},
		{
			name: "session variable with single quotes",
			hint: func() StatementHintUnion {
				var h StatementHintUnion
				h.SetValue(&SessionVariableHint{VariableName: "var", VariableValue: "it's"})
				return h
			}(),
			stmt:     "SELECT * FROM t WHERE x = 'hello'",
			database: tree.DNull,
			expected: `SELECT information_schema.crdb_set_session_variable_hint(e'SELECT * FROM t WHERE x = \'hello\'', 'var', e'it\'s');`,
			ok:       true,
		},
		{
			name: "session variable with backslash",
			hint: func() StatementHintUnion {
				var h StatementHintUnion
				h.SetValue(&SessionVariableHint{VariableName: "var", VariableValue: `a\b`})
				return h
			}(),
			stmt:     "SELECT 1",
			database: tree.DNull,
			expected: `SELECT information_schema.crdb_set_session_variable_hint('SELECT 1', 'var', e'a\\b');`,
			ok:       true,
		},
		{
			name:     "empty hint",
			hint:     StatementHintUnion{},
			stmt:     "SELECT 1",
			database: tree.DNull,
			ok:       false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := tc.hint.RecreateStmt(tc.stmt, tc.database)
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestFromToBytes(t *testing.T) {
	// Test writing empty hint.
	_, err := ToBytes(StatementHintUnion{})
	require.EqualError(t, err, "cannot convert empty hint to bytes")

	// Test reading empty bytes.
	_, err = FromBytes(nil)
	require.EqualError(t, err, "invalid hint bytes: no value set")
	_, err = FromBytes([]byte{})
	require.EqualError(t, err, "invalid hint bytes: no value set")

	// Test reading invalid bytes.
	_, err = FromBytes([]byte{0xFF, 0xFF, 0xFF})
	require.Error(t, err)

	// Test that a valid hint round trips.
	testRT := func(hint interface{}) {
		var hintUnion StatementHintUnion
		hintUnion.SetValue(hint)
		bytes, err := ToBytes(hintUnion)
		require.NoError(t, err)
		require.NotEmpty(t, bytes)
		decodedHintUnion, err := FromBytes(bytes)
		require.NoError(t, err)
		require.Equal(t, hint, decodedHintUnion.GetValue())
	}
	testRT(&InjectHints{})
	testRT(&InjectHints{DonorSQL: "SELECT * FROM t"})
	testRT(&SessionVariableHint{})
	testRT(&SessionVariableHint{VariableName: "distsql", VariableValue: "on"})
}
