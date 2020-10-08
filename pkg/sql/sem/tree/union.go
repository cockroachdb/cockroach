// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package tree

import "fmt"

// UnionClause represents a UNION statement.
type UnionClause struct {
	Type        UnionType
	Left, Right *Select
	All         bool
}

// UnionType represents one of the three set operations in sql.
type UnionType int

// Union.Type
const (
	UnionOp UnionType = iota
	IntersectOp
	ExceptOp
)

var unionTypeName = [...]string{
	UnionOp:     "UNION",
	IntersectOp: "INTERSECT",
	ExceptOp:    "EXCEPT",
}

func (i UnionType) String() string {
	if i < 0 || i > UnionType(len(unionTypeName)-1) {
		return fmt.Sprintf("UnionType(%d)", i)
	}
	return unionTypeName[i]
}

// Format implements the NodeFormatter interface.
func (node *UnionClause) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Left)
	ctx.WriteByte(' ')
	ctx.WriteString(node.Type.String())
	if node.All {
		ctx.WriteString(" ALL")
	}
	ctx.WriteByte(' ')
	ctx.FormatNode(node.Right)
}
