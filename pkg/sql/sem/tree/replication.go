// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

type IdentifySystem struct{}

func (i IdentifySystem) String() string {
	return AsString(&i)
}

func (i IdentifySystem) Format(ctx *FmtCtx) {
	ctx.WriteString("IDENTIFY_SYSTEM")
}

func (i IdentifySystem) StatementReturnType() StatementReturnType {
	return Rows
}

func (i IdentifySystem) StatementType() StatementType {
	return TypeDML
}

func (i IdentifySystem) StatementTag() string {
	return "IDENTIFY_SYSTEM"
}

type ReplicationOptions []ReplicationOption

func (r ReplicationOptions) Format(ctx *FmtCtx) {
	for i, o := range r {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&o)
	}
}

type StartReplication struct {
	SlotName Name
	// TODO(XXX): name is not strictly correct.
	// It doesn't allow LSNs to begin with 0s, which is wrong!
	LSNHigh, LSNLow Name
	Options         ReplicationOptions
}

type ReplicationOption struct {
	Option Name
	Value  Expr
}

func (r ReplicationOption) Format(ctx *FmtCtx) {
	ctx.FormatNode(&r.Option)
	ctx.WriteString(" ")
	ctx.FormatNode(r.Value)
}

func (s *StartReplication) String() string {
	return AsString(s)
}

func (s *StartReplication) Format(ctx *FmtCtx) {
	ctx.WriteString("START_REPLICATION SLOT ")
	ctx.FormatNode(&s.SlotName)
	ctx.WriteString(" LOGICAL ")
	ctx.FormatNode(&s.LSNHigh)
	ctx.WriteString("/")
	ctx.FormatNode(&s.LSNLow)
	if len(s.Options) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(s.Options)
		ctx.WriteString(")")
	}
}

func (s *StartReplication) StatementReturnType() StatementReturnType {
	return ReplMode
}

func (s *StartReplication) StatementType() StatementType {
	return TypeRepl
}

func (s *StartReplication) StatementTag() string {
	return "START_REPLICATION"
}
