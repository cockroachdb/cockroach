// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// This file contains facilities to report SQL activities to separate
// log channels.
//
// See the detailed log sink and format documentation
// (e.g. auto-generated files in docs/generated) for details about the
// general format of log entries.
//
// By default, the facilities in this file produce query logs
// using structured events. The payload of structured events
// is also auto-documented; see the corresponding event definitions
// for details.
//
// When the cluster setting `sql.log.unstructured_entries.enabled` is set
// (pre-v21.1 compatibility format, obsolete), the event payloads include
// the following fields:
//
//  - a label indicating where the data was generated - useful for troubleshooting.
//    - distinguishes e.g. exec, prepare, internal-exec, etc.
//  - the current value of `application_name`
//    - required for auditing, also helps filter out messages from a specific app.
//  - the logging trigger.
//    - "{}" for execution logs: any activity is worth logging in the exec log
//  - the full text of the query.
//  - the placeholder values. Useful for queries using placeholders.
//    - "{}" when there are no placeholders.
//  - the query execution time in milliseconds. For troubleshooting.
//  - the number of rows that were produced. For troubleshooting.
//  - the status of the query (OK for success, ERROR or full error
//    message upon error). Needed for auditing and troubleshooting.
//  - the number of times the statement was retried automatically
//    by the server so far.
//
// TODO(knz): Remove this documentation for the obsolete format when
// support for the format is removed, post-v21.1.

// logStatementsExecuteEnabled causes the Executor to log executed
// statements and, if any, resulting errors.
var logStatementsExecuteEnabled = settings.RegisterBoolSetting(
	"sql.trace.log_statement_execute",
	"set to true to enable logging of executed statements",
	false,
).WithPublic()

var slowQueryLogThreshold = settings.RegisterPublicDurationSettingWithExplicitUnit(
	"sql.log.slow_query.latency_threshold",
	"when set to non-zero, log statements whose service latency exceeds "+
		"the threshold to a secondary logger on each node",
	0,
	settings.NonNegativeDuration,
)

var slowInternalQueryLogEnabled = settings.RegisterBoolSetting(
	"sql.log.slow_query.internal_queries.enabled",
	"when set to true, internal queries which exceed the slow query log threshold "+
		"are logged to a separate log. Must have the slow query log enabled for this "+
		"setting to have any effect.",
	false,
).WithPublic()

var slowQueryLogFullTableScans = settings.RegisterBoolSetting(
	"sql.log.slow_query.experimental_full_table_scans.enabled",
	"when set to true, statements that perform a full table/index scan will be logged to the "+
		"slow query log even if they do not meet the latency threshold. Must have the slow query "+
		"log enabled for this setting to have any effect.",
	false,
).WithPublic()

var unstructuredQueryLog = settings.RegisterBoolSetting(
	"sql.log.unstructured_entries.enabled",
	"when set, SQL execution and audit logs use the pre-v21.1 unstrucured format",
	false,
)

var adminAuditLogEnabled = settings.RegisterBoolSetting(
	"sql.log.admin_audit.enabled",
	"when set, log SQL queries that are executed by a user with admin privileges",
	false,
)

type executorType int

const (
	executorTypeExec executorType = iota
	executorTypeInternal
)

// vLevel returns the vmodule log level at which logs from the given executor
// should be written to the logs.
func (s executorType) vLevel() log.Level { return log.Level(s) + 2 }

var logLabels = []string{"exec", "exec-internal"}

// logLabel returns the log label for the given executor type.
func (s executorType) logLabel() string { return logLabels[s] }

var sqlPerfLogger log.ChannelLogger = log.SqlPerf
var sqlPerfInternalLogger log.ChannelLogger = log.SqlInternalPerf

// maybeLogStatement conditionally records the current statement
// (p.curPlan) to the exec / audit logs.
func (p *planner) maybeLogStatement(
	ctx context.Context,
	execType executorType,
	numRetries, txnCounter, rows int,
	err error,
	queryReceived time.Time,
	hasAdminRoleCache *HasAdminRoleCache,
) {
	p.maybeLogStatementInternal(ctx, execType, numRetries, txnCounter, rows, err, queryReceived, hasAdminRoleCache)
}

func (p *planner) maybeLogStatementInternal(
	ctx context.Context,
	execType executorType,
	numRetries, txnCounter, rows int,
	err error,
	startTime time.Time,
	hasAdminRoleCache *HasAdminRoleCache,
) {
	// Note: if you find the code below crashing because p.execCfg == nil,
	// do not add a test "if p.execCfg == nil { do nothing }" !
	// Instead, make the logger work. This is critical for auditing - we
	// can't miss any statement.

	logV := log.V(2)
	logExecuteEnabled := logStatementsExecuteEnabled.Get(&p.execCfg.Settings.SV)
	slowLogThreshold := slowQueryLogThreshold.Get(&p.execCfg.Settings.SV)
	slowLogFullTableScans := slowQueryLogFullTableScans.Get(&p.execCfg.Settings.SV)
	slowQueryLogEnabled := slowLogThreshold != 0
	slowInternalQueryLogEnabled := slowInternalQueryLogEnabled.Get(&p.execCfg.Settings.SV)
	auditEventsDetected := len(p.curPlan.auditEvents) != 0

	// If hasAdminRoleCache IsSet is true iff AdminAuditLog is enabled.
	shouldLogToAdminAuditLog := hasAdminRoleCache.IsSet && hasAdminRoleCache.HasAdminRole

	// Only log to adminAuditLog if the statement is executed by
	// a user and the user has admin privilege (is directly or indirectly a
	// member of the admin role).

	if !logV && !logExecuteEnabled && !auditEventsDetected && !slowQueryLogEnabled &&
		!shouldLogToAdminAuditLog {
		// Shortcut: avoid the expense of computing anything log-related
		// if logging is not enabled by configuration.
		return
	}

	// Compute the pieces of data that are going to be included in logged events.

	// The session's application_name.
	appName := p.EvalContext().SessionData.ApplicationName
	// The duration of the query so far. Age is the duration expressed in milliseconds.
	queryDuration := timeutil.Now().Sub(startTime)
	age := float32(queryDuration.Nanoseconds()) / 1e6
	// The text of the error encountered, if the query did in fact end
	// in error.
	execErrStr := ""
	if err != nil {
		execErrStr = err.Error()
	}
	// The type of execution context (execute/prepare).
	lbl := execType.logLabel()

	if unstructuredQueryLog.Get(&p.execCfg.Settings.SV) {
		// This entire branch exists for the sake of backward
		// compatibility with log parsers for v20.2 and prior. This format
		// is obsolete and so this branch can be removed in v21.2.
		//
		// Look at the code "below" this if case for the main (default)
		// logging output.

		// The statement being executed.
		stmtStr := p.curPlan.stmt.AST.String()
		plStr := p.extendedEvalCtx.Placeholders.Values.String()

		if logV {
			// Copy to the debug log.
			log.VEventf(ctx, execType.vLevel(), "%s %q %q %s %.3f %d %q %d",
				lbl, appName, stmtStr, plStr, age, rows, execErrStr, numRetries)
		}

		// Now log!
		if auditEventsDetected {
			auditErrStr := "OK"
			if err != nil {
				auditErrStr = "ERROR"
			}

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
			logTrigger := buf.String()

			log.SensitiveAccess.Infof(ctx, "%s %q %s %q %s %.3f %d %s %d",
				lbl, appName, logTrigger, stmtStr, plStr, age, rows, auditErrStr, numRetries)
		}
		if slowQueryLogEnabled && (queryDuration > slowLogThreshold || slowLogFullTableScans) {
			logReason, shouldLog := p.slowQueryLogReason(queryDuration, slowLogThreshold)

			var logger log.ChannelLogger
			// Non-internal queries are always logged to the slow query log.
			if execType == executorTypeExec {
				logger = sqlPerfLogger
			}
			// Internal queries that surpass the slow query log threshold should only
			// be logged to the slow-internal-only log if the cluster setting dictates.
			if execType == executorTypeInternal && slowInternalQueryLogEnabled {
				logger = sqlPerfInternalLogger
			}

			if logger != nil && shouldLog {
				logger.Infof(ctx, "%.3fms %s %q {} %q %s %d %q %d %s",
					age, lbl, appName, stmtStr, plStr, rows, execErrStr, numRetries, logReason)
			}
		}
		if logExecuteEnabled {
			log.SqlExec.Infof(ctx, "%s %q {} %q %s %.3f %d %q %d",
				lbl, appName, stmtStr, plStr, age, rows, execErrStr, numRetries)
		}
		return
	}

	// New logging format in v21.1.
	sqlErrState := ""
	if err != nil {
		sqlErrState = pgerror.GetPGCode(err).String()
	}

	execDetails := eventpb.CommonSQLExecDetails{
		// Note: the current statement, application name, etc, are
		// automatically populated by the shared logic in event_log.go.
		ExecMode:      lbl,
		NumRows:       uint64(rows),
		SQLSTATE:      sqlErrState,
		ErrorText:     execErrStr,
		Age:           age,
		NumRetries:    uint32(numRetries),
		FullTableScan: p.curPlan.flags.IsSet(planFlagContainsFullTableScan),
		FullIndexScan: p.curPlan.flags.IsSet(planFlagContainsFullIndexScan),
		TxnCounter:    uint32(txnCounter),
	}

	if auditEventsDetected {
		// TODO(knz): re-add the placeholders and age into the logging event.
		entries := make([]eventLogEntry, len(p.curPlan.auditEvents))
		for i, ev := range p.curPlan.auditEvents {
			mode := "r"
			if ev.writing {
				mode = "rw"
			}
			tableName := ""
			if t, ok := ev.desc.(catalog.TableDescriptor); ok {
				// We only have a valid *table* name if the object being
				// audited is table-like (includes view, sequence etc). For
				// now, this is sufficient because the auditing feature can
				// only audit tables. If/when the mechanisms are extended to
				// audit databases and schema, we need more logic here to
				// extract a name to include in the logging events.
				tn, err := p.getQualifiedTableName(ctx, t)
				if err != nil {
					log.Warningf(ctx, "name for audited table ID %d not found: %v", ev.desc.GetID(), err)
				} else {
					tableName = tn.FQString()
				}
			}
			entries[i] = eventLogEntry{
				targetID: int32(ev.desc.GetID()),
				event: &eventpb.SensitiveTableAccess{
					CommonSQLExecDetails: execDetails,
					TableName:            tableName,
					AccessMode:           mode,
				},
			}
		}
		p.logEventsOnlyExternally(ctx, entries...)
	}

	if slowQueryLogEnabled && (
	// Did the user request pumping queries into the slow query log when
	// the logical plan has full scans?
	(slowLogFullTableScans && (execDetails.FullTableScan || execDetails.FullIndexScan)) ||
		// Is the query actually slow?
		queryDuration > slowLogThreshold) {
		switch {
		case execType == executorTypeExec:
			// Non-internal queries are always logged to the slow query log.
			p.logEventsOnlyExternally(ctx, eventLogEntry{event: &eventpb.SlowQuery{CommonSQLExecDetails: execDetails}})

		case execType == executorTypeInternal && slowInternalQueryLogEnabled:
			// Internal queries that surpass the slow query log threshold should only
			// be logged to the slow-internal-only log if the cluster setting dictates.
			p.logEventsOnlyExternally(ctx, eventLogEntry{event: &eventpb.SlowQueryInternal{CommonSQLExecDetails: execDetails}})
		}
	}

	if logExecuteEnabled || logV {
		// The API contract for logEventsWithOptions() is that it returns
		// no error when system.eventlog is not written to.
		_ = p.logEventsWithOptions(ctx,
			1, /* depth */
			eventLogOptions{
				// We pass LogToDevChannelIfVerbose because we have a log.V
				// request for this file, which means the operator wants to
				// see a copy of the execution on the DEV Channel.
				dst:               LogExternally | LogToDevChannelIfVerbose,
				verboseTraceLevel: execType.vLevel(),
			},
			eventLogEntry{event: &eventpb.QueryExecute{CommonSQLExecDetails: execDetails}})
	}

	if shouldLogToAdminAuditLog {
		p.logEventsOnlyExternally(ctx, eventLogEntry{event: &eventpb.AdminQuery{CommonSQLExecDetails: execDetails}})
	}
}

func (p *planner) logEventsOnlyExternally(ctx context.Context, entries ...eventLogEntry) {
	// The API contract for logEventsWithOptions() is that it returns
	// no error when system.eventlog is not written to.
	_ = p.logEventsWithOptions(ctx,
		2, /* depth: we want to use the caller location */
		eventLogOptions{dst: LogExternally},
		entries...)
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
func (p *planner) maybeAudit(desc catalog.Descriptor, priv privilege.Kind) {
	wantedMode := desc.GetAuditMode()
	if wantedMode == descpb.TableDescriptor_DISABLED {
		return
	}

	switch priv {
	case privilege.INSERT, privilege.DELETE, privilege.UPDATE:
		p.curPlan.auditEvents = append(p.curPlan.auditEvents, auditEvent{desc: desc, writing: true})
	default:
		p.curPlan.auditEvents = append(p.curPlan.auditEvents, auditEvent{desc: desc, writing: false})
	}
}

func (p *planner) slowQueryLogReason(
	queryDuration time.Duration, slowLogThreshold time.Duration,
) (reason string, shouldLog bool) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	sep := " "
	if slowLogThreshold != 0 && queryDuration > slowLogThreshold {
		fmt.Fprintf(&buf, "%sLATENCY_THRESHOLD", sep)
	}
	if p.curPlan.flags.IsSet(planFlagContainsFullTableScan) {
		fmt.Fprintf(&buf, "%sFULL_TABLE_SCAN", sep)
	}
	if p.curPlan.flags.IsSet(planFlagContainsFullIndexScan) {
		fmt.Fprintf(&buf, "%sFULL_SECONDARY_INDEX_SCAN", sep)
	}
	buf.WriteByte(' ')
	buf.WriteByte('}')
	reason = buf.String()
	return reason, reason != "{ }"
}

// auditEvent represents an audit event for a single table.
type auditEvent struct {
	// The descriptor being audited.
	desc catalog.Descriptor
	// Whether the event was for INSERT/DELETE/UPDATE.
	writing bool
}
