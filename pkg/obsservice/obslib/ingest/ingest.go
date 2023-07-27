// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ingest

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	otlogs "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4/pgxpool"
)

// EventIngester implements the OTLP Logs gRPC service, accepting connections
// and ingesting events.
type EventIngester struct {
	db *pgxpool.Pool
}

var _ logspb.LogsServiceServer = &EventIngester{}

func MakeEventIngester(ctx context.Context, cfg *pgxpool.Config) (EventIngester, error) {
	pool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		return EventIngester{}, errors.Wrap(err, "failed to connect to sink database")
	}
	return EventIngester{
		db: pool,
	}, nil
}

// Close closes the database connections opened by the ingester.
func (e EventIngester) Close() {
	e.db.Close()
}

// Export implements the LogsServiceServer gRPC service.
func (e *EventIngester) Export(
	ctx context.Context, request *logspb.ExportLogsServiceRequest,
) (*logspb.ExportLogsServiceResponse, error) {
	if e.db == nil {
		return nil, errors.AssertionFailedf("SetDB not called before incoming Export() RPC")
	}
	if err := persistEvents(ctx, request.ResourceLogs, e.db); err != nil {
		log.Warningf(ctx, "failed to persist events: %s", err)
	}
	return &logspb.ExportLogsServiceResponse{}, nil
}

// persistEvents writes events to the database.
func persistEvents(ctx context.Context, events []*otlogs.ResourceLogs, db *pgxpool.Pool) error {
	for _, group := range events {
		var clusterID uuid.UUID
		var nodeID roachpb.NodeID
		for _, att := range group.Resource.Attributes {
			switch att.Key {
			case obspb.ClusterID:
				var err error
				clusterID, err = uuid.FromString(att.Value.GetStringValue())
				if err != nil {
					log.Warningf(ctx, "invalid cluster ID: %s", att.Value)
					continue
				}
			case obspb.NodeID:
				nodeID = roachpb.NodeID(att.Value.GetIntValue())
				if nodeID == 0 {
					log.Warningf(ctx, "invalid node ID: %s", att.Value)
					continue
				}
			}
		}
		if clusterID.Equal(uuid.UUID{}) || nodeID == 0 {
			log.Warning(ctx, "clusterID or nodeID not set")
			continue
		}
		for _, scope := range group.ScopeLogs {
			switch scope.Scope.Name {
			case string(obspb.EventlogEvent):
				if err := persistEventlogEvents(ctx, scope, clusterID, nodeID, db); err != nil {
					log.Warningf(ctx, "error persisting events: %s", err)
					return err
				}
			}
		}
	}
	return nil
}

const maxEventsPerStatement = 100

// persistEventlogEvents writes "eventlog" events to the database.
func persistEventlogEvents(
	ctx context.Context,
	events otlogs.ScopeLogs,
	clusterID uuid.UUID,
	nodeID roachpb.NodeID,
	db *pgxpool.Pool,
) error {
	if events.Scope.Name != string(obspb.EventlogEvent) {
		panic(fmt.Sprintf("wrong event type: %s", events.Scope.Name))
	}

	// We're going to insert the events in chunks.
	evs := events.LogRecords
	for len(evs) > 0 {
		chunk := evs
		if len(evs) > maxEventsPerStatement {
			chunk = evs[:maxEventsPerStatement]
		}
		evs = evs[len(chunk):]

		// Build and execute a statement for the chunk.
		var sb strings.Builder
		_, _ = sb.WriteString("INSERT INTO cluster_events(timestamp, cluster_id, instance_id, event_type, event) VALUES ")
		const colsPerEvent = 5
		args := make([]interface{}, 0, len(chunk)*colsPerEvent)
		argNum := 1
		for i, ev := range chunk {
			var eventType string
			for _, kv := range ev.Attributes {
				if kv.Key == obspb.EventlogEventTypeAttribute {
					eventType = kv.Value.GetStringValue()
				}
			}
			data := ev.Body.GetStringValue()
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
				argNum, argNum+1, argNum+2, argNum+3, argNum+4))
			argNum += 5
			args = append(args,
				timeutil.Unix(0, int64(ev.TimeUnixNano)),
				clusterID,
				nodeID,
				eventType,
				data)
		}

		log.VEventf(ctx, 2, "inserting %d events", len(chunk))
		_, err := db.Exec(ctx, sb.String(), args...)
		if err != nil {
			return err
		}
	}
	return nil
}
