// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

// PauseJob represents a PAUSE JOB statement.
type PauseJob struct {
	ID Expr
}

// Format implements the NodeFormatter interface.
func (node *PauseJob) Format(ctx *FmtCtx) {
	ctx.WriteString("PAUSE JOB ")
	ctx.FormatNode(node.ID)
}

// ResumeJob represents a RESUME JOB statement.
type ResumeJob struct {
	ID Expr
}

// Format implements the NodeFormatter interface.
func (node *ResumeJob) Format(ctx *FmtCtx) {
	ctx.WriteString("RESUME JOB ")
	ctx.FormatNode(node.ID)
}

// CancelJob represents a CANCEL JOB statement.
type CancelJob struct {
	ID Expr
}

// Format implements the NodeFormatter interface.
func (node *CancelJob) Format(ctx *FmtCtx) {
	ctx.WriteString("CANCEL JOB ")
	ctx.FormatNode(node.ID)
}

// CancelQuery represents a CANCEL QUERY statement.
type CancelQuery struct {
	ID       Expr
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *CancelQuery) Format(ctx *FmtCtx) {
	ctx.WriteString("CANCEL QUERY ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.ID)
}

// CancelSession represents a CANCEL SESSION statement.
type CancelSession struct {
	ID       Expr
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *CancelSession) Format(ctx *FmtCtx) {
	ctx.WriteString("CANCEL SESSION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.ID)
}
