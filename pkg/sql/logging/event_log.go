// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logging

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obs/logstream"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type StructuredEventProcessor struct {
	execCfg *sql.ExecutorConfig
}

func NewStructuredEventProcessor(execCfg *sql.ExecutorConfig) StructuredEventProcessor {
	return StructuredEventProcessor{
		execCfg: execCfg,
	}
}

func (s *StructuredEventProcessor) RegisterEventType(
	ctx context.Context, event logpb.EventPayload,
) {
	eventName := logpb.GetEventTypeName(event)
	meta := log.StructuredLogMeta{EventType: log.EventType(eventName), Version: "0.1"}
	logstream.RegisterProcessor(ctx, s.execCfg.Stopper, meta, s)
}

func (s *StructuredEventProcessor) Process(ctx context.Context, event any) error {
	if !s.isEventLogEnabled() {
		return nil
	}
	e, ok := event.(logpb.EventPayload)
	if !ok {
		panic(errors.AssertionFailedf("Unexpected event type provided to SQLStatsLogProcessor: %v", event))
	}
	query, args := s.prepareEventWrite(ctx, e)
	if err := timeutil.RunWithTimeout(context.TODO(), "record-events", 5*time.Second, func(ctx context.Context) error {
		return s.execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := txn.ExecEx(ctx, "record-events", txn.KV(), sessiondata.NodeUserSessionDataOverride, query, args...)
			if err != nil {
				log.Warningf(ctx, "failed to record events: %#v.", err)
			}
			return err
		})
	}); err != nil {
		log.Ops.Warningf(ctx, "unable to save %d entries to system.eventlog: %v", 1, err)
		return err
	}

	return nil
}

var _ logstream.Processor = (*StructuredEventProcessor)(nil)

func (s *StructuredEventProcessor) isEventLogEnabled() bool {
	return sql.EventLogSystemTableEnabled.Get(&s.execCfg.Settings.SV)
}

func (s *StructuredEventProcessor) prepareEventWrite(
	ctx context.Context, entries ...logpb.EventPayload,
) (query string, args []interface{}) {
	reportingID := s.execCfg.NodeInfo.NodeID.SQLInstanceID()
	const colsPerEvent = 4
	// Note: we insert the value zero as targetID because sadly this
	// now-deprecated column has a NOT NULL constraint.
	// TODO(knz): Add a migration to remove the column altogether.
	const baseQuery = `
INSERT INTO system.eventlog (
  timestamp, "eventType", "reportingID", info, "targetID"
)
VALUES($1, $2, $3, $4, 0)`
	args = make([]interface{}, 0, len(entries)*colsPerEvent)
	for i := 0; i < len(entries); i++ {
		event := entries[i]

		infoBytes := redact.RedactableBytes("{")
		_, infoBytes = event.AppendJSONFields(false /* printComma */, infoBytes)
		infoBytes = append(infoBytes, '}')
		// In the system.eventlog table, we do not use redaction markers.
		// (compatibility with previous versions of CockroachDB.)
		infoBytes = infoBytes.StripMarkers()
		eventType := event.CommonDetails().EventType
		args = append(
			args,
			timeutil.Unix(0, event.CommonDetails().Timestamp),
			eventType,
			reportingID,
			string(infoBytes),
		)
	}

	// In the common case where we have just 1 event, we want to skeep
	// the extra heap allocation and buffer operations of the loop
	// below. This is an optimization.
	query = baseQuery
	if len(entries) > 1 {
		// Extend the query with additional VALUES clauses for all the
		// events after the first one.
		var completeQuery strings.Builder
		completeQuery.WriteString(baseQuery)

		for i := range entries[1:] {
			placeholderNum := 1 + colsPerEvent*(i+1)
			fmt.Fprintf(&completeQuery, ", ($%d, $%d, $%d, $%d, 0)",
				placeholderNum, placeholderNum+1, placeholderNum+2, placeholderNum+3)
		}
		query = completeQuery.String()
	}

	return query, args
}
