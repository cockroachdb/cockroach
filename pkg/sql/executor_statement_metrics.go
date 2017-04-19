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
//

package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// sessionPhase is used to index the Session.phaseTimes array.
type sessionPhase int

const (
	// When the session is created (pgwire). Used to compute
	// the session age.
	sessionInit sessionPhase = iota

	// When a batch of SQL code is received in pgwire.
	// Used to compute the batch age.
	sessionStartBatch

	// Executor phases.
	sessionStartParse
	sessionEndParse
	plannerStartLogicalPlan
	plannerEndLogicalPlan
	plannerStartExecStmt
	plannerEndExecStmt

	// sessionNumPhases must be listed last so that it can be used to
	// define arrays sufficiently large to hold all the other values.
	sessionNumPhases
)

// phaseTimes is the type of the session.phaseTimes array.
type phaseTimes [sessionNumPhases]time.Time

// recordStatementSummery gathers various details pertaining to the
// last executed statement/query and performs the associated
// accounting.
// - distSQLUsed reports whether the query was distributed.
// - automaticRetryCount is the count of implicit txn retries
//   so far.
// - result is the result set computed by the query/statement.
// - err is the error encountered, if any.
func (e *Executor) recordStatementSummary(
	planner *planner,
	stmt parser.Statement,
	distSQLUsed bool,
	automaticRetryCount int,
	result Result,
	err error,
) {
	phaseTimes := &planner.phaseTimes

	// Compute the run latency. This is always recorded in the
	// server metrics.
	runLatRaw := phaseTimes[plannerEndExecStmt].Sub(phaseTimes[plannerStartExecStmt])

	// Collect the statistics.
	numRows := result.RowsAffected
	if result.Type == parser.Rows {
		numRows = result.Rows.Len()
	}

	runLat := runLatRaw.Seconds()

	parseLat := phaseTimes[sessionEndParse].
		Sub(phaseTimes[sessionStartParse]).Seconds()
	planLat := phaseTimes[plannerEndLogicalPlan].
		Sub(phaseTimes[plannerStartLogicalPlan]).Seconds()
	// service latency: start to parse to end of run
	svcLatRaw := phaseTimes[plannerEndExecStmt].Sub(phaseTimes[sessionStartParse])
	svcLat := svcLatRaw.Seconds()

	// processing latency: contributing towards SQL results.
	processingLat := parseLat + planLat + runLat

	// overhead latency: txn/retry management, error checking, etc
	execOverhead := svcLat - processingLat

	if automaticRetryCount == 0 {
		if distSQLUsed {
			if _, ok := stmt.(*parser.Select); ok {
				e.DistSQLSelectCount.Inc(1)
			}
			e.DistSQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
			e.DistSQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
		} else {
			e.SQLExecLatency.RecordValue(runLatRaw.Nanoseconds())
			e.SQLServiceLatency.RecordValue(svcLatRaw.Nanoseconds())
		}
	}

	planner.session.appStats.recordStatement(
		stmt, distSQLUsed, automaticRetryCount, numRows, err,
		parseLat, planLat, runLat, svcLat, execOverhead,
	)

	if log.V(2) {
		// ages since significant epochs
		batchAge := phaseTimes[plannerEndExecStmt].
			Sub(phaseTimes[sessionStartBatch]).Seconds()
		sessionAge := phaseTimes[plannerEndExecStmt].
			Sub(phaseTimes[sessionInit]).Seconds()

		log.Infof(planner.session.Ctx(),
			"query stats: %d rows, %d retries, "+
				"parse %.2fµs (%.1f%%), "+
				"plan %.2fµs (%.1f%%), "+
				"run %.2fµs (%.1f%%), "+
				"overhead %.2fµs (%.1f%%), "+
				"batch age %.3fms, session age %.4fs",
			numRows, automaticRetryCount,
			parseLat*1e6, 100*parseLat/svcLat,
			planLat*1e6, 100*planLat/svcLat,
			runLat*1e6, 100*runLat/svcLat,
			execOverhead*1e6, 100*execOverhead/svcLat,
			batchAge*1000, sessionAge,
		)
	}
}
