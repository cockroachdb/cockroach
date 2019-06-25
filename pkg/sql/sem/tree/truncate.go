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

// Truncate represents a TRUNCATE statement.
type Truncate struct {
	Tables       TableNames
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *Truncate) Format(ctx *FmtCtx) {
	ctx.WriteString("TRUNCATE TABLE ")
	sep := ""
	for i := range node.Tables {
		ctx.WriteString(sep)
		ctx.FormatNode(&node.Tables[i])
		sep = ", "
	}
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(node.DropBehavior.String())
	}
}
