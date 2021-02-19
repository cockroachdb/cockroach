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
	"github.com/stretchr/testify/require"
)

func TestFmt(t *testing.T) {
	var p parser.Parser
	stmts, _ := p.Parse("SELECT 1::int8; SELECT 1::pg_catalog.int8; SELECT 1::schem.typ; SELECT 1::schem.typ[];")
	s := stmts.StringWithFlags(tree.FmtAnonymize)

	// is there a way to avoid redacting pg_catalog.int8?
	require.Equal(t, "SELECT 1::INT8; SELECT 1::_._; SELECT 1::_._; SELECT 1::_._[]", s)
}
