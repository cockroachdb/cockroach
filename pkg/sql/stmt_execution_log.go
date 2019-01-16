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

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// executionLog holds statement executions which have yet to be written
// to the system table.
type executionLog struct {
	st *cluster.Settings

	syncutil.Mutex
	stmtExecutions []stmtExecution
}

// stmtExecution holds per-statement timings.
type stmtExecution struct {
	receivedAt          time.Time
	statement           string
	applicationName     string
	distributed         bool
	optimized           bool
	automaticRetryCount int
	error               string
	rowsAffected        int
	parseLat            time.Duration
	planLat             time.Duration
	runLat              time.Duration
	serviceLat          time.Duration
}

// stmtExecutionLogEnable determines whether to collect per-execution
// timings.
var stmtExecutionLogEnable = settings.RegisterBoolSetting(
	"sql.metrics.statement_execution_log.enabled", "collect per-execution statement timings", true,
)

// sqlExecutionLogCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var sqlExecutionLogCollectionLatencyThreshold = settings.RegisterDurationSetting(
	"sql.metrics.statement_execution_log.threshold",
	"minimum execution time to cause statistics to be collected",
	0,
)

// sqlExecutionLogFlushFrequency specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var sqlExecutionLogFlushFrequency = settings.RegisterDurationSetting(
	"sql.metrics.statement_execution_log.flush_frequency",
	"the frequency to flush statement executions to the system table",
	1*time.Second,
)

// makeExecutionLog creates an execution log for the given cluster settings.
func makeExecutionLog(st *cluster.Settings) executionLog {
	return executionLog{
		st:             st,
		stmtExecutions: make([]stmtExecution, 0, 100), // TODO: what initial cap?
	}
}

// recordExecution saves per-execution timings.
func (e *executionLog) recordExecution(
	statement string,
	applicationName string,
	receivedAt time.Time,
	distributed bool,
	optimized bool,
	automaticRetryCount int,
	rowsAffected int,
	err error,
	parseLat, planLat, runLat, serviceLat time.Duration,
) {
	if e == nil || !stmtExecutionLogEnable.Get(&e.st.SV) {
		return
	}

	if t := sqlExecutionLogCollectionLatencyThreshold.Get(&e.st.SV); t > 0 && t >= serviceLat {
		return
	}

	var errorMessage string
	if err != nil {
		errorMessage = err.Error()
	}

	stmtExecution := stmtExecution{
		receivedAt:          receivedAt,
		statement:           statement,
		applicationName:     applicationName,
		distributed:         distributed,
		optimized:           optimized,
		automaticRetryCount: automaticRetryCount,
		error:               errorMessage,
		rowsAffected:        rowsAffected,
		parseLat:            parseLat,
		planLat:             planLat,
		runLat:              runLat,
		serviceLat:          serviceLat,
	}

	e.Lock()
	e.stmtExecutions = append(e.stmtExecutions, stmtExecution)
	e.Unlock()
}

// writeToSql stores the in-memory execution log to the SQL table and
// clears the in-memory store.
func (e *executionLog) writeToSQL(ctx context.Context, execCfg *ExecutorConfig) {
	e.Lock()
	executions := e.stmtExecutions
	e.stmtExecutions = make([]stmtExecution, 0, len(e.stmtExecutions)/2)
	e.Unlock()

	insertTimings := `INSERT INTO system.statement_executions
(
    received_at, node_id, statement, application_name,
    distributed, optimized, automatic_retry_count,
    error, rows_affected,
    parse_lat, plan_lat, run_lat, service_lat
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
)`

	// TODO: bulkify this DML
	for i := range executions {
		ex := executions[i]
		if _ /* rows */, err := execCfg.InternalExecutor.Exec(
			ctx,
			"flush-statement-execution-log",
			nil, /* txn */
			insertTimings,
			ex.receivedAt,
			execCfg.NodeID.Get(),
			ex.statement,
			ex.applicationName,
			ex.distributed,
			ex.optimized,
			ex.automaticRetryCount,
			ex.error,
			ex.rowsAffected,
			ex.parseLat,
			ex.planLat,
			ex.runLat,
			ex.serviceLat,
		); err != nil {
			log.Warningf(ctx, "Unable to flush execution log: %v", err)
		}
	}
}
