// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

// BinaryOpName is a mapping from all binary operators that are supported by
// the vectorized engine to their names.
var BinaryOpName = map[tree.BinaryOperatorSymbol]string{
	tree.Bitand:            "Bitand",
	tree.Bitor:             "Bitor",
	tree.Bitxor:            "Bitxor",
	tree.Plus:              "Plus",
	tree.Minus:             "Minus",
	tree.Mult:              "Mult",
	tree.Div:               "Div",
	tree.FloorDiv:          "FloorDiv",
	tree.Mod:               "Mod",
	tree.Pow:               "Pow",
	tree.Concat:            "Concat",
	tree.LShift:            "LShift",
	tree.RShift:            "RShift",
	tree.JSONFetchVal:      "JSONFetchVal",
	tree.JSONFetchText:     "JSONFetchText",
	tree.JSONFetchValPath:  "JSONFetchValPath",
	tree.JSONFetchTextPath: "JSONFetchTextPath",
}

// ComparisonOpName is a mapping from all comparison operators that are
// supported by the vectorized engine to their names.
var ComparisonOpName = map[treecmp.ComparisonOperatorSymbol]string{
	treecmp.EQ: "EQ",
	treecmp.NE: "NE",
	treecmp.LT: "LT",
	treecmp.LE: "LE",
	treecmp.GT: "GT",
	treecmp.GE: "GE",
}
