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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var auditLogEnabled = settings.RegisterBoolSetting("sql.auditlog.enabled", "if set, a separate SQL audit log is produced", false)
var execLogEnabled = settings.RegisterBoolSetting("sql.execlog.enabled", "if set, a separate SQL execution log is produced", false)

func (e *Executor) maybeLogStatement(
	ctx context.Context, session *Session, planner *planner, stmt Statement, rows int, err error) {
	if auditLogEnabled.Get(&e.cfg.Settings.SV) {
		// For the audit log, we only log the tag, row count and whether there was an error
		// (not the error itself); this prevents leaking of PII.
		e.auditLog.Logf(ctx, "audit %q - %s -- %d %v",
			session.data.ApplicationName(), stmt.AST.StatementTag(), rows, err != nil)
	}
	if execLogEnabled.Get(&e.cfg.Settings.SV) {
		// For the execution log, we log the full statement, time elapsed
		// so far, row count, and if an error occurred, the full text of
		// the error. This helps troubleshooting.
		age := timeutil.Now().Sub(planner.statsCollector.PhaseTimes()[sessionStartParse])
		e.execLog.Logf(ctx, "exec %q - %s -- %.3fms %d %v",
			session.data.ApplicationName(), stmt.String(), float64(age.Nanoseconds())/1e6, rows, err)
	}
}
