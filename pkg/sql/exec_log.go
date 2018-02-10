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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

	logV := log.V(2)
	logExecuteEnabled := logStatementsExecuteEnabled.Get(&s.settings.SV)
	auditEventsDetected := len(p.curPlan.auditEvents) != 0

	if !logV && !logExecuteEnabled && !auditEventsDetected {
		return
	}

	// Logged data, in order:

	// label passed as argument.

	appName := s.ApplicationName()

	logTrigger := "{}"
	if auditEventsDetected {
		var buf bytes.Buffer
		buf.WriteByte('{')
		sep := ""
		for _, ev := range p.curPlan.auditEvents {
			mode := "READ"
			if ev.writing {
				mode = "READWRITE"
			}
			fmt.Fprintf(&buf, "%s%q[%d]:%s", sep, ev.desc.GetName(), ev.desc.GetID(), mode)
			sep = ", "
		}
		buf.WriteByte('}')
		logTrigger = buf.String()
	}

	stmtStr := p.curPlan.AST.String()

	plStr := p.extendedEvalCtx.Placeholders.Values.String()

	age := float64(timeutil.Now().Sub(startTime).Nanoseconds()) / 1e6

	// rows passed as argument.

	execErrStr := ""
	auditErrStr := "OK"
	if err != nil {
		execErrStr = err.Error()
		auditErrStr = "ERROR"
	}

	// Now log!
	if auditEventsDetected {
		logger := p.execCfg.AuditLogger
		logger.Logf(ctx, "%s %q %s %q %s %.3f %d %s",
			lbl, appName, logTrigger, stmtStr, plStr, age, rows, auditErrStr)
	}
	if logExecuteEnabled {
		logger := p.execCfg.ExecLogger
		logger.Logf(ctx, "%s %q %s %q %s %.3f %d %q",
			lbl, appName, logTrigger, stmtStr, plStr, age, rows, execErrStr)
	}
	if logV {
		// Copy to the main log.
		log.VEventf(ctx, 2, "%s %q %s %q %s %.3f %d %q",
			lbl, appName, logTrigger, stmtStr, plStr, age, rows, execErrStr)
	}
}

// maybeAudit marks the current plan being constructed as flagged
// for auditing if the table being touched has an auditing mode set.
// This is later picked up by maybeLogStatement() above.
//
// It is crucial that this gets checked reliably -- we don't want to
// miss any statements! For now, we call this from CheckPrivilege(),
// as this is the function most likely to be called reliably from any
// caller that also uses a descriptor. Future changes that move the
// call to this method elsewhere must find a way to ensure that
// contributors who later add features do not have to remember to call
// this to get it right.
func (p *planner) maybeAudit(desc sqlbase.DescriptorProto, priv privilege.Kind) {
	wantedMode := desc.GetAuditMode()
	if wantedMode == sqlbase.TableDescriptor_DISABLED {
		return
	}

	switch priv {
	case privilege.INSERT, privilege.DELETE, privilege.UPDATE:
		p.curPlan.auditEvents = append(p.curPlan.auditEvents, auditEvent{desc: desc, writing: true})
	default:
		p.curPlan.auditEvents = append(p.curPlan.auditEvents, auditEvent{desc: desc, writing: false})
	}
}

// auditEvent represents an audit event for a single table.
type auditEvent struct {
	// The descriptor being audited.
	desc sqlbase.DescriptorProto
	// Whether the event was for INSERT/DELETE/UPDATE.
	writing bool
}
