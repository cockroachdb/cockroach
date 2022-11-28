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
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	otlogs "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EventIngester connects to a CRDB node through the SubscribeToEvents RPC and
// saves the incoming events to a database.
type EventIngester struct{}

// StartIngestEvents runs event ingestion in a stopper task.
func (e *EventIngester) StartIngestEvents(
	ctx context.Context, addr string, db *pgxpool.Pool, stop *stop.Stopper,
) {
	_ = stop.RunAsyncTask(ctx, "event ingester", func(ctx context.Context) {
		ctx, cancel := stop.WithCancelOnQuiesce(ctx)
		defer cancel()
		e.ingestEvents(ctx, addr, db)
	})
}

// ingestEvents subscribes to events published by the CRDB node at addr and
// persists them to the database.
//
// The call blocks until the RPC is terminated by the server or ctx is canceled.
func (e *EventIngester) ingestEvents(ctx context.Context, addr string, db *pgxpool.Pool) {
	// TODO(andrei): recover from connection errors.
	// TODO(andrei): use certs for secure clusters.
	log.Shoutf(ctx, logpb.Severity_INFO, "DIALING")
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithBlock(), // block until the connection is established
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("failed to dial %s: %s", addr, err))
	}
	defer func() {
		_ = conn.Close() // nolint:grpcconnclose
	}()
	log.Infof(ctx, "Ingesting events from %s.", addr)
	log.Shoutf(ctx, logpb.Severity_INFO, "Ingesting events from %s.", addr)

	c := obspb.NewObsClient(conn)
	log.Shoutf(ctx, logpb.Severity_INFO, "Subscribing")
	stream, err := c.SubscribeToEvents(ctx,
		&obspb.SubscribeToEventsRequest{
			Identity: "Obs Service",
		})
	if err != nil {
		panic(fmt.Sprintf("SubscribeToEvents call to %s failed: %s", addr, err))
	}
	log.Shoutf(ctx, logpb.Severity_INFO, "Subscribed")

	for {
		events, err := stream.Recv()
		if err != nil {
			if (err != io.EOF && ctx.Err() == nil) || log.V(2) {
				log.Infof(ctx, "event stream error: %s", err)
			}
			// TODO(andrei): recover from the error by trying to reestablish the
			// connection.
			return
		}
		if true { //log.V(3) {
			log.Shoutf(ctx, logpb.Severity_INFO, "received events: %s", events.String())
		}
		err = persistEvents(ctx, events.ResourceLogs, db)
		if err != nil {
			log.Errorf(ctx, "error persisting events: %s", err)
		}
	}
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
			case string(obspb.ExecutionInsightEvent):
				if err := persistExecutionInsightEvents(ctx, scope, clusterID, nodeID, db); err != nil {
					log.Shoutf(ctx, logpb.Severity_ERROR, "error persisting events: %s", err)
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

func persistExecutionInsightEvents(
	ctx context.Context, events otlogs.ScopeLogs, id uuid.UUID, id2 roachpb.NodeID, db *pgxpool.Pool,
) error {
	for _, event := range events.LogRecords {
		insight := insights.Insight{}

		err := protoutil.Unmarshal(event.Body.GetBytesValue(), &insight)

		_, err = db.Exec(context.Background(),
			`INSERT INTO execution_insights (
                            timestamp,
                            session_id,
                            txn_id,
                            txn_fingerprint_id,
                            stmt_id,
                            stmt_fingerprint_id,
                            problem,
                            causes,
                            query,
                            status,
                            start_time,
                            end_time,
                            full_scan,
                            user_name,
                            app_name,
                            database_name,
                            plan_gist,
                            rows_read,
                            rows_written,
                            priority,
                            retries,
                            last_retry_reason,
                            exec_node_ids,
                            contention,
                            contention_events,
                            index_recommendations,
                            implicit_txn
                          )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27 );`,
			timeutil.Unix(0, int64(event.TimeUnixNano)),
			insight.Session.ID.String(),
			insight.Transaction.ID.String(),
			encodeUint64ToBytes(uint64(insight.Transaction.FingerprintID)),
			hex.EncodeToString(insight.Statement.ID.GetBytes()),
			encodeUint64ToBytes(uint64(insight.Statement.FingerprintID)),
			insight.Problem.String(),
			encodeCauses(insight.Causes),
			insight.Statement.Query,
			insight.Statement.Status.String(),
			timeutil.Unix(0, insight.Statement.StartTime.UnixNano()),
			timeutil.Unix(0, insight.Statement.EndTime.UnixNano()),
			insight.Statement.FullScan,
			insight.Statement.User,
			insight.Statement.ApplicationName,
			insight.Statement.Database,
			insight.Statement.PlanGist,
			insight.Statement.RowsRead,
			insight.Statement.RowsWritten,
			insight.Transaction.UserPriority,
			insight.Statement.Retries,
			// TODO(abarganier): Do we need to use something like sql.NullString here?
			insight.Statement.AutoRetryReason,
			insight.Statement.Nodes,
			encodeContentionTime(insight.Statement.Contention),
			encodeContentionEvents(insight.Statement.ContentionEvents),
			encodeIndexRecommendations(insight.Statement.IndexRecommendations),
			insight.Transaction.ImplicitTxn,
		)

		if err != nil {
			return err
		}
	}
	return nil
}

func encodeCauses(causes []insights.Cause) []string {
	out := make([]string, len(causes))
	for i := 0; i < len(causes); i++ {
		out[i] = causes[i].String()
	}
	return out
}

func encodeContentionEvents(events []roachpb.ContentionEvent) interface{} {
	// TODO(todd): Is this the correct way to encode to JSON? Does it
	// match the format when querying the same row in crdb_internal?
	jsonpb := protoutil.JSONPb{}
	json, err := jsonpb.Marshal(events)
	if err != nil {
		panic(err)
	}
	// TODO(abarganier): Seems like if the JSON is null, it resolves to the string 'null'
	// instead of an actual nil value. What's a better way to handle this?
	if len(json) == 0 || string(json) == "null" {
		return nil
	}
	return string(json)
}

func encodeContentionTime(contention *time.Duration) interface{} {
	return contention
}

func encodeIndexRecommendations(recommendations []string) []string {
	if recommendations == nil {
		return []string{}
	}
	return recommendations
}

func encodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}
