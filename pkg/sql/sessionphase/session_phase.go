// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessionphase

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
)

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
	// Executor phases.

	// SessionQueryReceived is the SessionPhase when a query is received.
	SessionQueryReceived = iota

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

	// SessionTransactionStarted is the SessionPhase when a transaction is
	// started.
	SessionTransactionStarted

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
	initTime time.Time
	times    [SessionNumPhases]crtime.Mono
}

// NewTimes create a new instance of the Times.
func NewTimes() *Times {
	return &Times{initTime: timeutil.Now()}
}

// SetSessionPhaseTime sets the time for a given SessionPhase.
func (t *Times) SetSessionPhaseTime(sp SessionPhase, time crtime.Mono) {
	t.times[sp] = time
}

// GetSessionPhaseTime retrieves the time for a given SessionPhase.
func (t *Times) GetSessionPhaseTime(sp SessionPhase) crtime.Mono {
	return t.times[sp]
}

// InitTime is the time when this Times instance was created (which should
// coincide with the time the session was initialized).
func (t *Times) InitTime() time.Time {
	return t.initTime
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
	var queryReceivedTime crtime.Mono
	if t.times[SessionEndParse] == 0 {
		queryReceivedTime = t.times[SessionStartParse]
	} else {
		queryReceivedTime = t.times[SessionQueryReceived]
	}
	parseLatency := t.times[SessionEndParse].Sub(queryReceivedTime)
	// If we encounter an error during the logical planning, the
	// PlannerEndExecStmt phase will not be set, so we need to use the end of
	// planning phase in the computation of planAndExecuteLatency.
	var queryEndExecTime crtime.Mono
	if t.times[PlannerEndExecStmt] == 0 {
		queryEndExecTime = t.times[PlannerEndLogicalPlan]
	} else {
		queryEndExecTime = t.times[PlannerEndExecStmt]
	}
	planAndExecuteLatency := queryEndExecTime.Sub(t.times[PlannerStartLogicalPlan])
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
	return t.times[SessionEndExecTransaction].Sub(t.times[SessionTransactionStarted])
}

// GetCommitLatency returns the total time spent for the transaction to
// finish commit.
func (t *Times) GetCommitLatency() time.Duration {
	return t.times[SessionEndTransactionCommit].Sub(t.times[SessionStartTransactionCommit])
}

// GetSessionAge returns the age of the current session since initialization.
func (t *Times) GetSessionAge() time.Duration {
	return t.times[PlannerEndExecStmt].Sub(crtime.MonoFromTime(t.InitTime()))
}

// GetIdleLatency deduces the rough amount of time spent waiting for the client
// while the transaction is open. (For implicit transactions, this value is 0.)
func (t *Times) GetIdleLatency(previous *Times) time.Duration {
	queryReceived := t.times[SessionQueryReceived]

	var previousQueryReceived crtime.Mono
	var previousQueryServiced crtime.Mono
	if previous != nil {
		previousQueryReceived = previous.times[SessionQueryReceived]
		previousQueryServiced = previous.times[SessionQueryServiced]
	}

	// If we were received at the same time as the previous execution
	// (i.e., as part of a compound statement), we didn't have to wait
	// for the client at all.
	if queryReceived == previousQueryReceived {
		return 0
	}

	// In general, we have been waiting for the client since the end
	// of the previous execution.
	waitingSince := previousQueryServiced

	transactionStarted := t.times[SessionTransactionStarted]
	// Transaction started is not set. Assume it's an implicit
	// transaction so there is no idle time.
	if transactionStarted == 0 {
		return 0
	}

	// Although we really only want to measure idle latency *within*
	// an open transaction. So if we're in a new transaction, measure
	// from its start time instead.
	if transactionStarted > waitingSince {
		waitingSince = transactionStarted
	}

	// And if we were received before we started waiting for the client,
	// then there is no idle latency at all.
	if waitingSince > queryReceived {
		return 0
	}

	return queryReceived.Sub(waitingSince)
}
