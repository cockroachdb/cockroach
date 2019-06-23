// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// ControlJobs represents a PAUSE/RESUME/CANCEL JOBS statement.
type ControlJobs struct {
	Jobs    *Select
	Command JobCommand
}

// JobCommand determines which type of action to effect on the selected job(s).
type JobCommand int

// JobCommand values
const (
	PauseJob JobCommand = iota
	CancelJob
	ResumeJob
)

// JobCommandToStatement translates a job command integer to a statement prefix.
var JobCommandToStatement = map[JobCommand]string{
	PauseJob:  "PAUSE",
	CancelJob: "CANCEL",
	ResumeJob: "RESUME",
}

// Format implements the NodeFormatter interface.
func (n *ControlJobs) Format(ctx *FmtCtx) {
	ctx.WriteString(JobCommandToStatement[n.Command])
	ctx.WriteString(" JOBS ")
	ctx.FormatNode(n.Jobs)
}

// CancelQueries represents a CANCEL QUERIES statement.
type CancelQueries struct {
	Queries  *Select
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *CancelQueries) Format(ctx *FmtCtx) {
	ctx.WriteString("CANCEL QUERIES ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Queries)
}

// CancelSessions represents a CANCEL SESSIONS statement.
type CancelSessions struct {
	Sessions *Select
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *CancelSessions) Format(ctx *FmtCtx) {
	ctx.WriteString("CANCEL SESSIONS ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Sessions)
}
