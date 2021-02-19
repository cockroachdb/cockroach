// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestFmtTypeNameAnonymize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var p parser.Parser
	for _, testCase := range []struct {
		input    string
		expected string
	}{
		{
			input:    `SELECT 1::int8`,
			expected: `SELECT 1::INT8`,
		},
		{
			input:    `SELECT 1::integer`,
			expected: `SELECT 1::INT8`,
		},
		{
			// It would be nice to detect that there's nothing to anonymize here,
			// but doing so would require a big refactor to FormatTypeReference
			input:    `SELECT 1::pg_catalog.int8`,
			expected: `SELECT 1::_._`,
		},
		{
			input:    `SELECT 1::schem.typ`,
			expected: `SELECT 1::_._`,
		},
		{
			input:    `SELECT 1::schem.typ[];`,
			expected: `SELECT 1::_._[]`,
		},
	} {
		stmts, _ := p.Parse(testCase.input)
		actual := stmts.StringWithFlags(tree.FmtAnonymize)
		require.Equal(t, testCase.expected, actual)
	}
}
