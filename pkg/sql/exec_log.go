// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// logStatementsExecuteEnabled causes the Executor to log executed
// statements and, if any, resulting errors.
var logStatementsExecuteEnabled = settings.RegisterBoolSetting(
	"sql.trace.log_statement_execute",
	"set to true to enable logging of executed statements",
	false,
)

var splitExecLog = settings.RegisterBoolSetting("sql.trace.log_statement_execute.split",
	"if set, the execution log enabled by sql.trace.log_statement_execute or vmodule will also go to separate log files", false)

// maybeLogStatement conditionally records the current statemenbt
// (p.curPlan) to the exec / audit logs.
func (p *planner) maybeLogStatement(ctx context.Context, lbl string, rows int, err error) {
	p.maybeLogStatementInternal(
		ctx, lbl, rows, err, p.statsCollector.PhaseTimes()[sessionStartParse])
}

func (p *planner) maybeLogStatementInternal(
	ctx context.Context,
	lbl string,
	rows int,
	err error,
	startTime time.Time,
) {
	// Note: if you find the code below crashing because p.execCfg == nil,
	// do not add a test "if p.execCfg == nil { do nothing }" !
	// Instead, make the logger work. This is critical for auditing - we
	// can't miss any statement.

	s := &p.sessionDataMutator
	// Are we emitting to the execution log?
	if log.V(2) || logStatementsExecuteEnabled.Get(&s.settings.SV) {
		logger := p.execCfg.ExecLogger

		// For the execution log, we log:
		// - a label about the caller;
		// - the application name;
		// - the full statement, the placeholder values if any,
		// - time elapsed so far (milliseconds),
		// - result row count,
		// - if an error occurred, the full text of the error.
		appName := s.ApplicationName()
		stmtStr := p.curPlan.AST.String()
		plStr := p.extendedEvalCtx.Placeholders.Values.String()
		age := float64(timeutil.Now().Sub(startTime).Nanoseconds()) / 1e6
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}

		// The string fields below are quoted to facilitate parsing,
		// except for the placeholder string which already contains
		// quoted strings.

		if splitExecLog.Get(&s.settings.SV) {
			logger.Logf(ctx, "%s %q %q %s %.3f %d %q", lbl, appName, stmtStr, plStr, age, rows, errStr)
		} else {
			// Copy to the main log.
			log.VEventf(ctx, 2, "%s %q %q %s %.3f %d %q", lbl, appName, stmtStr, plStr, age, rows, errStr)
		}
	}
}
