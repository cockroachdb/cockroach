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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var auditLogEnabled = settings.RegisterBoolSetting("sql.auditlog.enabled", "if set, a separate SQL audit log is produced", false)
var execLogEnabled = settings.RegisterBoolSetting("sql.execlog.enabled", "if set, a separate SQL execution log is produced", false)

func (e *Executor) maybeLogStatement(
	ctx context.Context, session *Session, planner *planner, stmt Statement, rows int, err error) {
	// Are we emitting to the audit log?
	if len(planner.curPlan.auditEvents) != 0 && auditLogEnabled.Get(&e.cfg.Settings.SV) {
		// Descripe the list of objects.
		var buf bytes.Buffer
		sep := ""
		for _, e := range planner.curPlan.auditEvents {
			mode := "READ"
			if e.writing {
				mode = "READWRITE"
			}
			fmt.Fprintf(&buf, "%s%q[%d]:%s", sep, e.desc.GetName(), e.desc.GetID(), mode)
			sep = ", "
		}
		ok := "OK"
		if err != nil {
			ok = "ERROR"
		}

		// For the audit log, we only log the tag, row count and whether there was an error
		// (not the error itself); this prevents leaking of PII.
		e.auditLog.Logf(ctx, "audit %q - %s - %s -- %d %s",
			session.data.ApplicationName(), buf.String(), stmt.AST.StatementTag(), rows, ok)
	}

	// Are we emitting to the execution log?
	if execLogEnabled.Get(&e.cfg.Settings.SV) {
		// For the execution log, we log the full statement, time elapsed
		// so far, row count, and if an error occurred, the full text of
		// the error. This helps troubleshooting.
		age := timeutil.Now().Sub(planner.statsCollector.PhaseTimes()[sessionStartParse])
		e.execLog.Logf(ctx, "exec %q -- %s -- %.3fms %d %v",
			session.data.ApplicationName(), stmt.String(), float64(age.Nanoseconds())/1e6, rows, err)
	}
}

// checkAuditMode marks the current plan being constructed as flagged
// for auditing. This is picked up by maybeLogStatement() above.
//
// It is crucial that this gets checked reliably -- we don't want to
// miss any statements! For now, we call this from CheckPrivilege(),
// as this is the function most likely to be called reliably from any
// caller that also uses a descriptor. Future changes that move the
// call to this method elsewhere must find a way to ensure that
// contributors who later add features do not have to remember to call
// this to get it right.
func (p *planner) checkAuditMode(desc sqlbase.DescriptorProto, priv privilege.Kind) {
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

type auditEvent struct {
	// The descriptor being audited.
	desc sqlbase.DescriptorProto
	// Whether the event was for INSERT/DELETE/UPDATE.
	writing bool
}
