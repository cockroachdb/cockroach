// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// This code was derived from https://github.com/youtube/vitess.

package tree

// ValuesClause represents a VALUES clause.
type ValuesClause struct {
	Rows []Exprs
}

// Format implements the NodeFormatter interface.
func (node *ValuesClause) Format(ctx *FmtCtx) {
	ctx.WriteString("VALUES ")
	comma := ""
	for i := range node.Rows {
		ctx.WriteString(comma)
		ctx.WriteByte('(')
		ctx.FormatNode(&node.Rows[i])
		ctx.WriteByte(')')
		comma = ", "
	}
}

// ValuesClauseWithNames is a VALUES clause that has been annotated with column
// names. This is only produced at plan time, never by the parser. It's used to
// pass column names to the VALUES planNode, so it can produce intelligible
// error messages during value type checking.
type ValuesClauseWithNames struct {
	ValuesClause

	// Names is a list of the column names that each tuple in the ValuesClause
	// corresponds to.
	Names NameList
}
