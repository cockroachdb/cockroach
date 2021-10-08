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

// Times is the data structure that keep tracks of the time for each
// SessionPhase.
type Times struct {
	times [SessionNumPhases]time.Time
}

// NewTimes create a new instance of the Times.
func NewTimes() *Times {
	return &Times{}
}

// SetSessionPhaseTime sets the time for a given SessionPhase.
func (t *Times) SetSessionPhaseTime(sp SessionPhase, time time.Time) {
	t.times[sp] = time
}

// GetSessionPhaseTime retrieves the time for a given SessionPhase.
func (t *Times) GetSessionPhaseTime(sp SessionPhase) time.Time {
	return t.times[sp]
}

// Clone returns a copy of the current PhaseTimes.
func (t *Times) Clone() *Times {
	tCopy := &Times{}
	*tCopy = *t
	return tCopy
}

// GetServiceLatencyNoOverhead returns the latency of serving a query
// excluding miscellaneous sources of the overhead (e.g. internal retries).
// This method is safe to call if SessionQueryServiced phase hasn't been set
// yet.
func (t *Times) GetServiceLatencyNoOverhead() time.Duration {
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
	if t.times[SessionEndParse].IsZero() {
		queryReceivedTime = t.times[SessionStartParse]
	} else {
		queryReceivedTime = t.times[SessionQueryReceived]
	}
	parseLatency := t.times[SessionEndParse].Sub(queryReceivedTime)
	planAndExecuteLatency := t.times[PlannerEndExecStmt].Sub(t.times[PlannerStartLogicalPlan])
	return parseLatency + planAndExecuteLatency
}

// GetServiceLatencyTotal returns the total latency of serving a query
// including any overhead like internal retries.
// NOTE: SessionQueryServiced phase must have been set.
func (t *Times) GetServiceLatencyTotal() time.Duration {
	return t.times[SessionQueryServiced].Sub(t.times[SessionQueryReceived])
}

// GetRunLatency returns the time between a query execution starting and
// ending.
func (t *Times) GetRunLatency() time.Duration {
	return t.times[PlannerEndExecStmt].Sub(t.times[PlannerStartExecStmt])
}

// GetPlanningLatency returns the time it takes for a query to be planned.
func (t *Times) GetPlanningLatency() time.Duration {
	return t.times[PlannerEndLogicalPlan].Sub(t.times[PlannerStartLogicalPlan])
}

// GetParsingLatency returns the time it takes for a query to be parsed.
func (t *Times) GetParsingLatency() time.Duration {
	return t.times[SessionEndParse].Sub(t.times[SessionStartParse])
}

// GetPostCommitJobsLatency returns the time spent running the post
// transaction commit jobs, such as schema changes.
func (t *Times) GetPostCommitJobsLatency() time.Duration {
	return t.times[SessionEndPostCommitJob].Sub(t.times[SessionStartPostCommitJob])
}

// GetTransactionRetryLatency returns the time that was spent retrying the
// transaction.
func (t *Times) GetTransactionRetryLatency() time.Duration {
	return t.times[SessionMostRecentStartExecTransaction].Sub(t.times[SessionFirstStartExecTransaction])
}

// GetTransactionServiceLatency returns the total time to service the
// transaction.
func (t *Times) GetTransactionServiceLatency() time.Duration {
	return t.times[SessionEndExecTransaction].Sub(t.times[SessionTransactionReceived])
}

// GetCommitLatency returns the total time spent for the transaction to
// finish commit.
func (t *Times) GetCommitLatency() time.Duration {
	return t.times[SessionEndTransactionCommit].Sub(t.times[SessionStartTransactionCommit])
}

// GetSessionAge returns the age of the current session since initialization.
func (t *Times) GetSessionAge() time.Duration {
	return t.times[PlannerEndExecStmt].Sub(t.times[SessionInit])
}
