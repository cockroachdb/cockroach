// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// ControlJobs represents a PAUSE/RESUME/CANCEL JOBS statement.
type ControlJobs struct {
	Jobs    *Select
	Command JobCommand
	Reason  Expr
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
	if n.Reason != nil {
		ctx.WriteString(" WITH REASON = ")
		ctx.FormatNode(n.Reason)
	}
}

// AlterJobOwner represents an ALTER JOB OWNER TO statement.
type AlterJobOwner struct {
	Job   Expr
	Owner RoleSpec
}

// Format implements the NodeFormatter interface.
func (n *AlterJobOwner) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER JOB ")
	ctx.FormatNode(n.Job)
	ctx.WriteString(" OWNER TO ")
	ctx.FormatNode(&n.Owner)
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

// ScheduleCommand determines which type of action to effect on the selected job(s).
type ScheduleCommand int

// ScheduleCommand values
const (
	PauseSchedule ScheduleCommand = iota
	ResumeSchedule
	DropSchedule
)

func (c ScheduleCommand) String() string {
	switch c {
	case PauseSchedule:
		return "PAUSE"
	case ResumeSchedule:
		return "RESUME"
	case DropSchedule:
		return "DROP"
	default:
		panic("unhandled schedule command")
	}
}

// ControlSchedules represents PAUSE/RESUME SCHEDULE statement.
type ControlSchedules struct {
	Schedules *Select
	Command   ScheduleCommand
}

var _ Statement = &ControlSchedules{}

// Format implements the NodeFormatter interface.
func (n *ControlSchedules) Format(ctx *FmtCtx) {
	ctx.WriteString(n.Command.String())
	ctx.WriteString(" SCHEDULES ")
	ctx.FormatNode(n.Schedules)
}

// ControlJobsForSchedules represents PAUSE/RESUME/CANCEL clause
// which applies job command to the jobs matching specified schedule(s).
type ControlJobsForSchedules struct {
	Schedules *Select
	Command   JobCommand
}

// ControlJobsOfType represents PAUSE/RESUME/CANCEL clause which
// applies the job command to the job matching a specified type
type ControlJobsOfType struct {
	Type    string
	Command JobCommand
}

// Format implements the NodeFormatter interface.
func (n *ControlJobsOfType) Format(ctx *FmtCtx) {
	ctx.WriteString(JobCommandToStatement[n.Command])
	ctx.WriteString(" ALL ")
	ctx.WriteString(n.Type)
	ctx.WriteString(" JOBS")
}

// Format implements NodeFormatter interface.
func (n *ControlJobsForSchedules) Format(ctx *FmtCtx) {
	ctx.WriteString(JobCommandToStatement[n.Command])
	ctx.WriteString(" JOBS FOR SCHEDULES ")
	ctx.FormatNode(n.Schedules)
}

var _ Statement = &ControlJobsForSchedules{}
var _ Statement = &ControlJobsOfType{}
