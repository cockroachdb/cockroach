// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessionphase

import "time"

// SQL execution is separated in 3+ phases:
// - parse/prepare
// - plan
// - run
//
// The commonly used term "execution latency" encompasses this entire
// process. However for the purpose of analyzing / optimizing
// individual parts of the SQL execution engine, it is useful to
// separate the durations of these individual phases. The code below
// does this.

// SessionPhase is used to index the Session.PhaseTimes array.
type SessionPhase int

const (
	// SessionInit is the SessionPhase the session is created (pgwire). It is used
	// to compute the session age.
	SessionInit SessionPhase = iota

	// Executor phases.

	// SessionQueryReceived is the SessionPhase when a query is received.
	SessionQueryReceived

	// SessionStartParse is the SessionPhase when parsing starts.
	SessionStartParse

	// SessionEndParse is the SessionPhase when parsing ends.
	SessionEndParse

	// PlannerStartLogicalPlan is the SessionPhase when planning starts.
	PlannerStartLogicalPlan

	// PlannerEndLogicalPlan is the SessionPhase when planning ends.
	PlannerEndLogicalPlan

	// PlannerStartExecStmt is the SessionPhase when execution starts.
	PlannerStartExecStmt

	// PlannerEndExecStmt is the SessionPhase when execution ends.
	PlannerEndExecStmt

	// SessionQueryServiced is the SessionPhase when a query is serviced.
	// Note: we compute this even for empty queries or  "special" statements that
	// have no execution, like SHOW TRANSACTION STATUS.
	SessionQueryServiced

	// SessionTransactionReceived is the SessionPhase when a transaction is
	// received.
	SessionTransactionReceived

	// SessionFirstStartExecTransaction is the SessionPhase when a transaction
	// is started for the first time.
	SessionFirstStartExecTransaction

	// SessionMostRecentStartExecTransaction is the SessionPhase when a
	// transaction is started for the most recent time.
	SessionMostRecentStartExecTransaction

	// SessionEndExecTransaction is the SessionPhase when a transaction is either
	// committed or rolled back.
	SessionEndExecTransaction

	// SessionStartTransactionCommit is the SessionPhase when a transaction
	// `COMMIT` starts.
	SessionStartTransactionCommit

	// SessionEndTransactionCommit is the SessionPhase when a transaction `COMMIT`
	// ends.
	SessionEndTransactionCommit

	// SessionStartPostCommitJob is the SessionPhase when a post transaction
	// `COMMIT` job starts.
	SessionStartPostCommitJob

	// SessionEndPostCommitJob is the SessionPhase when a post transaction
	// `COMMIT`	job ends.
	SessionEndPostCommitJob

	// SessionNumPhases must be listed last so that it can be used to
	// define arrays sufficiently large to hold all the other values.
	SessionNumPhases
)

type PhaseTimes interface {
	// SetSessionPhaseTime sets the time for a given SessionPhase.
	SetSessionPhaseTime(phase SessionPhase, time time.Time)

	// GetSessionPhaseTime retrieves the time for a given SessionPhase.
	GetSessionPhaseTime(phase SessionPhase) time.Time

	// Clone returns a copy of the current PhaseTimes.
	Clone() PhaseTimes

	// GetServiceLatencyNoOverhead returns the latency of serving a query
	// excluding miscellaneous sources of the overhead (e.g. internal retries).
	// This method is safe to call if SessionQueryServiced phase hasn't been set
	// yet.
	GetServiceLatencyNoOverhead() time.Duration

	// GetServiceLatencyTotal returns the total latency of serving a query
	// including any overhead like internal retries.
	// NOTE: SessionQueryServiced phase must have been set.
	GetServiceLatencyTotal() time.Duration

	// GetRunLatency returns the time between a query execution starting and
	// ending.
	GetRunLatency() time.Duration

	// GetPlanningLatency returns the time it takes for a query to be planned.
	GetPlanningLatency() time.Duration

	// GetParsingLatency returns the time it takes for a query to be parsed.
	GetParsingLatency() time.Duration

	// GetPostCommitJobsLatency returns the time spent running the post
	// transaction commit jobs, such as schema changes.
	GetPostCommitJobsLatency() time.Duration

	// GetTransactionRetryLatency returns the time that was spent retrying the
	// transaction.
	GetTransactionRetryLatency() time.Duration

	// GetTransactionServiceLatency returns the total time to service the
	// transaction.
	GetTransactionServiceLatency() time.Duration

	// GetCommitLatency returns the total time spent for the transaction to
	// finish commit.
	GetCommitLatency() time.Duration

	// GetSessionAge returns the age of the current session since initialization.
	GetSessionAge() time.Duration
}

// phaseTimes implements sessionphase.PhaseTimes interface.
// It's important that this is an array and not a slice, as we rely on the array
// copy behavior.
type phaseTimes [SessionNumPhases]time.Time

var _ PhaseTimes = &phaseTimes{}

// NewPhaseTime create a new instance of the PhaseTimes.
func NewPhaseTime() PhaseTimes {
	return &phaseTimes{}
}

// SetSessionPhaseTime implements the PhaseTimes interface.
func (p *phaseTimes) SetSessionPhaseTime(sp SessionPhase, time time.Time) {
	p[sp] = time
}

// GetSessionPhaseTime implements the PhaseTimes interface.
func (p *phaseTimes) GetSessionPhaseTime(sp SessionPhase) time.Time {
	return p[sp]
}

// Clone implements the PhaseTimes interface.
func (p *phaseTimes) Clone() PhaseTimes {
	pCopy := &phaseTimes{}
	*pCopy = *p
	return pCopy
}

// GetServiceLatencyNoOverhead implements the PhaseTimes interface.
func (p *phaseTimes) GetServiceLatencyNoOverhead() time.Duration {
	// To have an accurate representation of how long it took to service this
	// single query, we ignore the time between when parsing ends and planning
	// begins. This avoids the latency being inflated in a few different cases:
	// when there are internal transaction retries, and when multiple statements
	// are submitted together, e.g. "SELECT 1; SELECT 2".
	//
	// If we're executing a portal, both parsing start and end times will be
	// zero, so subtracting the actual time at which the query was received will
	// produce a negative value which doesn't make sense. Therefore, we "fake"
	// the received time to be the parsing start time.
	var queryReceivedTime time.Time
	if p[SessionEndParse].IsZero() {
		queryReceivedTime = p[SessionStartParse]
	} else {
		queryReceivedTime = p[SessionQueryReceived]
	}
	parseLatency := p[SessionEndParse].Sub(queryReceivedTime)
	planAndExecuteLatency := p[PlannerEndExecStmt].Sub(p[PlannerStartLogicalPlan])
	return parseLatency + planAndExecuteLatency
}

// GetServiceLatencyTotal implements the PhaseTimes interface.
func (p *phaseTimes) GetServiceLatencyTotal() time.Duration {
	return p[SessionQueryServiced].Sub(p[SessionQueryReceived])
}

// GetRunLatency implements the PhaseTimes interface.
func (p *phaseTimes) GetRunLatency() time.Duration {
	return p[PlannerEndExecStmt].Sub(p[PlannerStartExecStmt])
}

// GetPlanningLatency implements the PhaseTimes interface.
func (p *phaseTimes) GetPlanningLatency() time.Duration {
	return p[PlannerEndLogicalPlan].Sub(p[PlannerStartLogicalPlan])
}

// GetParsingLatency implements the PhaseTimes interface.
func (p *phaseTimes) GetParsingLatency() time.Duration {
	return p[SessionEndParse].Sub(p[SessionStartParse])
}

// GetPostCommitJobsLatency implements the PhaseTimes interface.
func (p *phaseTimes) GetPostCommitJobsLatency() time.Duration {
	return p[SessionEndPostCommitJob].Sub(p[SessionStartPostCommitJob])
}

// GetTransactionRetryLatency implements the PhaseTimes interface.
func (p *phaseTimes) GetTransactionRetryLatency() time.Duration {
	return p[SessionMostRecentStartExecTransaction].Sub(p[SessionFirstStartExecTransaction])
}

// GetTransactionServiceLatency implements the PhaseTimes interface.
func (p *phaseTimes) GetTransactionServiceLatency() time.Duration {
	return p[SessionEndExecTransaction].Sub(p[SessionTransactionReceived])
}

// GetCommitLatency implements the PhaseTimes interface.
func (p *phaseTimes) GetCommitLatency() time.Duration {
	return p[SessionEndTransactionCommit].Sub(p[SessionStartTransactionCommit])
}

// GetSessionAge implements the PhaseTimes interface.
func (p *phaseTimes) GetSessionAge() time.Duration {
	return p[PlannerEndExecStmt].Sub(p[SessionInit])
}
