// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/stretchr/testify/require"
)

func TestParseDeclareSection(t *testing.T) {
	fn := `
DECLARE
BEGIN
END
`
	stmt, err := parser.Parse(fn)
	require.NoError(t, err)
	require.NotNil(t, stmt.AST)
}
