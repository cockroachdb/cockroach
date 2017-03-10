// Copyright 2015 The Cockroach Authors.
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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// sessionPhase is used to index the Session.phaseTimes array.
type sessionPhase int

const (
	sessionInit sessionPhase = iota
	sessionStartBatch
	sessionStartParse
	sessionEndParse
	sessionStartLogicalPlan
	sessionEndLogicalPlan
	sessionStartExecStmt
	sessionEndExecStmt
	// sessionNumPhases must be listed last so that it can be used to
	// define arrays sufficiently large to hold all the other values.
	sessionNumPhases
)

func (e *Executor) recordStatementMetrics(
	ctx context.Context,
	stmt parser.Statement,
	phaseTimes []time.Time,
	distributedQuery bool,
	retryCount int,
	result Result,
	err error,
) {

	// SQL execution is separated in 3+ phases:
	// - parse/prepare
	// - plan
	// - run
	//
	// The commonly used term "execution latency" encompasses this
	// entire process. However for the purpose of analyzing / optimizing
	// individual parts of the SQL execution engine,
	// it is useful to separate the durations of these individual phases.

	// Compute the run latency. This is always recorded in the
	// server metrics.
	runLat := phaseTimes[sessionEndExecStmt].Sub(phaseTimes[sessionStartExecStmt]).Nanoseconds()

	if distributedQuery {
		if _, ok := stmt.(*parser.Select); ok {
			e.DistSQLSelectCount.Inc(1)
		}
		e.DistSQLExecLatency.RecordValue(runLat)
	} else {
		e.SQLExecLatency.RecordValue(runLat)
	}

	// Conditionally report the other metrics.
	// TODO(knz) #13968.
	if log.V(2) {
		runLatf := float64(runLat)

		parseLatf := float64(phaseTimes[sessionEndParse].
			Sub(phaseTimes[sessionStartParse]).Nanoseconds())
		planLatf := float64(phaseTimes[sessionEndLogicalPlan].
			Sub(phaseTimes[sessionStartLogicalPlan]).Nanoseconds())
		// execution latency: start to parse to end of run
		execLatf := float64(phaseTimes[sessionEndExecStmt].
			Sub(phaseTimes[sessionStartParse]).Nanoseconds())

		// processing latency: the computing work towards SQL results
		processingLatf := parseLatf + planLatf + runLatf
		// overhead latency: txn/retry management, error checking, etc
		execOverheadf := execLatf - processingLatf

		// ages since significant epochs
		batchAge := float64(phaseTimes[sessionEndExecStmt].
			Sub(phaseTimes[sessionStartBatch]).Nanoseconds())
		sessionAge := float64(phaseTimes[sessionEndExecStmt].
			Sub(phaseTimes[sessionInit]).Nanoseconds())

		numRows := result.RowsAffected
		if result.Type == parser.Rows {
			numRows = result.Rows.Len()
		}

		log.Infof(ctx, "query stats: %d rows, %d retries, "+
			"parse %.2fµs (%.1f%%), "+
			"plan %.2fµs (%.1f%%), "+
			"run %.2fµs (%.1f%%), "+
			"overhead %.2fµs (%.2f%%), "+
			"batch age %.3fms, session age %.4fs",
			numRows, retryCount,
			parseLatf/1000, 100*parseLatf/execLatf,
			planLatf/1000, 100*planLatf/execLatf,
			runLatf/1000, 100*runLatf/execLatf,
			execOverheadf/1000, 100*execOverheadf/execLatf,
			batchAge/1e6, sessionAge/1e9,
		)
	}
}
