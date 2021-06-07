// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRedactCCLStatement verifies that CCL statements are redacted
// correctly
func TestRedactCCLStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		query    string
		expected string
	}{
		{
			"IMPORT CSV 'file' WITH delimiter = 'foo'",
			"IMPORT CSV ‹'file'› WITH delimiter = ‹'foo'›",
		},
	}

	for _, test := range testCases {
		stmt, err := parser.ParseOne(test.query)
		if err != nil {
			t.Fatal(err)
		}
		annotations := tree.MakeAnnotations(stmt.NumAnnotations)
		f := tree.NewFmtCtxEx(tree.FmtAlwaysQualifyTableNames|tree.FmtRedactNode, &annotations)
		f.FormatNode(stmt.AST)
		redactedString := f.CloseAndGetString()
		require.Equal(t, test.expected, redactedString)
	}
}
