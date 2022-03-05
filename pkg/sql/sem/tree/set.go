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

// SetVar represents a SET or RESET statement.
type SetVar struct {
	Name     string
	Local    bool
	Values   Exprs
	Reset    bool
	ResetAll bool
}

// Format implements the NodeFormatter interface.
func (node *SetVar) Format(ctx *FmtCtx) {
	if node.ResetAll {
		ctx.WriteString("RESET ALL")
		return
	}
	if node.Reset {
		ctx.WriteString("RESET ")
		ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
			// Session var names never contain PII and should be distinguished
			// for feature tracking purposes.
			ctx.FormatNameP(&node.Name)
		})
		return
	}
	ctx.WriteString("SET ")
	if node.Local {
		ctx.WriteString("LOCAL ")
	}
	if node.Name == "" {
		ctx.WriteString("ROW (")
		ctx.FormatNode(&node.Values)
		ctx.WriteString(")")
	} else {
		ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
			// Session var names never contain PII and should be distinguished
			// for feature tracking purposes.
			ctx.FormatNameP(&node.Name)
		})

		ctx.WriteString(" = ")
		ctx.FormatNode(&node.Values)
	}
}

// SetClusterSetting represents a SET CLUSTER SETTING statement.
type SetClusterSetting struct {
	Name  string
	Value Expr
}

// Format implements the NodeFormatter interface.
func (node *SetClusterSetting) Format(ctx *FmtCtx) {
	ctx.WriteString("SET CLUSTER SETTING ")

	// Cluster setting names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
		ctx.FormatNameP(&node.Name)
	})

	ctx.WriteString(" = ")

	switch v := node.Value.(type) {
	case *DBool, *DInt:
		ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
			ctx.FormatNode(v)
		})
	default:
		ctx.FormatNode(v)
	}
}

// SetTransaction represents a SET TRANSACTION statement.
type SetTransaction struct {
	Modes TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *SetTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("SET TRANSACTION")
	ctx.FormatNode(&node.Modes)
}

// SetSessionAuthorizationDefault represents a SET SESSION AUTHORIZATION DEFAULT
// statement. This can be extended (and renamed) if we ever support names in the
// last position.
type SetSessionAuthorizationDefault struct{}

// Format implements the NodeFormatter interface.
func (node *SetSessionAuthorizationDefault) Format(ctx *FmtCtx) {
	ctx.WriteString("SET SESSION AUTHORIZATION DEFAULT")
}

// SetSessionCharacteristics represents a SET SESSION CHARACTERISTICS AS TRANSACTION statement.
type SetSessionCharacteristics struct {
	Modes TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *SetSessionCharacteristics) Format(ctx *FmtCtx) {
	ctx.WriteString("SET SESSION CHARACTERISTICS AS TRANSACTION")
	ctx.FormatNode(&node.Modes)
}

// SetTracing represents a SET TRACING statement.
type SetTracing struct {
	Values Exprs
}

// Format implements the NodeFormatter interface.
func (node *SetTracing) Format(ctx *FmtCtx) {
	ctx.WriteString("SET TRACING = ")
	// Set tracing values never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags&^FmtMarkRedactionNode, func() {
		ctx.FormatNode(&node.Values)
	})
}
