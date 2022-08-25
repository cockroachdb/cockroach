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
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/leasing"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	otlogs "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EventIngester connects to a CRDB node through the SubscribeToEvents RPC and
// saves the incoming events to a database.
type EventIngester struct {
	db   *pgxpool.Pool
	stop *stop.Stopper
	// leaseMgr is used to acquire exclusive leases on monitoring targets.
	//
	// Note that the EventIngester does not own the Session. This means, for
	// example, that the manager might be Stop()ed while the EventIngester is
	// running, causing lease acquisitions to fail.
	leaseMgr *leasing.Session
}

// MakeEventIngester constructs an EventIngester.
func MakeEventIngester(
	db *pgxpool.Pool, stop *stop.Stopper, leaseMgr *leasing.Session,
) EventIngester {
	return EventIngester{
		db:       db,
		stop:     stop,
		leaseMgr: leaseMgr,
	}
}

// StartIngestEvents runs event ingestion in a stopper task.
func (e *EventIngester) StartIngestEvents(ctx context.Context, targetID int, addr string) error {
	return e.stop.RunAsyncTask(ctx, "event ingester", func(ctx context.Context) {
		ctx = logtags.AddTag(ctx, "target", targetID)
		ctx = logtags.AddTag(ctx, "addr", addr)
		ctx, cancel := e.stop.WithCancelOnQuiesce(ctx)
		defer cancel()
		err := e.ingestEvents(ctx, targetID, addr, e.db)
		if err != nil {
			// TODO(andrei): Handle errors.
			panic(err)
		}
	})
}

// ingestEvents subscribes to events published by the CRDB node at addr and
// persists them to the database.
//
// The call blocks until ctx is canceled.
func (e *EventIngester) ingestEvents(
	ctx context.Context, targetID int, addr string, db *pgxpool.Pool,
) error {
	fmt.Printf("Ingesting events from %s.\n", addr)

	// Attempt to connect to the target. We do this early, so that we don't take a
	// lease if we can't connect.
	conn, err := grpcDial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.close()

	leased := false
	// Each loop iteration corresponds to a successful reconnect to the target.
	for ctx.Err() == nil {
		// If the connection is not in an established state, we're going to wait
		// until it reconnects (this is done by gRPC mostly transparently).
		// Depending on whether we have a lease or not, we'll wait for the
		// reconnection with a timeout. If we have a lease, we don't want to block
		// too long before releasing it. If we can't connect to the target, maybe
		// other nodes can, so we want to allow other to take the lease.
		var timeout time.Duration
		if leased {
			timeout = 5 * time.Second
		}
		ok := conn.connect(ctx, timeout)
		if !ok {
			// If we didn't manage to reconnect within the timeout, release the lease.
			// This way we don't hold on to the lease for a target that we can't
			// connect to. If we manage to connect in the future, we'll acquire a new
			// lease.
			_ /* err */ = e.leaseMgr.Release(ctx, targetID)
			leased = false

			// Loop around to attempt to connect again. Next attempt will not have a
			// timeout.
			continue
		}

		// Acquire a lease on this target, if we don't already have one. This lease
		// gives us exclusive access to pull data from the monitoring target.
		if !leased {
			_, err = e.leaseMgr.Lease(ctx, targetID)
			if err != nil {
				// TODO(andrei): Rationalize the error handling.
				switch {
				case errors.Is(err, leasing.ErrSessionStopped):
					return err
				case errors.Is(err, leasing.ErrLeasedByAnother):
					return err
				default:
					return err
				}
			}
		}

		// Now that we have a lease, we start pulling data from the node. Note that
		// ingestEventsInner() is not aware of leasing, thus it might continue
		// pulling data even if the lease expires. That's OK; as long as we can get
		// data from the target, we might as well get it - lease or no lease.
		// However, if another Obs Service node get the lease and connects to the
		// target, the target will accept that connection and disconnect us.
		//
		// Also note that there is theoretically a race here whereby, if the lease
		// we checked above expires before we connect to the target, our connection
		// might kick out another node's connection while that node has a valid
		// lease. We don't attempt to prevent that. If it happens, that other node
		// will retry its connection and disconnect us, and we won't retry (since
		// we'll recheck our invalid lease).
		if err := e.ingestEventsInner(ctx, conn.conn, db); err != nil {
			status, ok := status.FromError(err)
			// TODO(andrei): CRDB eventually returns a code Unavailable just before it
			// restarts, but not while it rejects new RPCs because the stopper is
			// draining. During the draining phase, it return "node unavailable; try
			// another peer" (i.e. the message from NodeUnavailableError) with code
			// Unknown. Figure out if CRDB should return the Unavailable code for that
			// error, or if we should handle the respective error string here.
			if ok && status.Code() == codes.Unavailable {
				log.Infof(ctx, "target %d at %s is going away", targetID, addr)
				// CRDB is communicating that it's shutting down. Let's give it a bit of
				// time to restart.
				select {
				case <-ctx.Done():
				case <-time.After(3 * time.Second):
				}
			} else {
				log.Warningf(ctx, "error ingesting events: %s", err)
			}
		}
	}
	return nil
}

// ingestEventsInner subscribes to events published by the CRDB node at addr and
// persists them to the database.
//
// The call blocks until the RPC is terminated by the server or ctx is canceled.
func (e *EventIngester) ingestEventsInner(
	ctx context.Context, conn *grpc.ClientConn, db *pgxpool.Pool,
) error {
	c := obspb.NewObsClient(conn)
	// Make the RPC call with a timeout.
	stream, err := c.SubscribeToEvents(ctx,
		&obspb.SubscribeToEventsRequest{Identity: "Obs Service"},
	)
	if err != nil {
		return errors.Wrap(err, "SubscribeToEvents call failed")
	}
	log.VEventf(ctx, 2, "established successful RPC connection")

	for {
		events, err := stream.Recv()
		if err != nil {
			if (err != io.EOF && ctx.Err() == nil) || log.V(2) {
				log.Infof(ctx, "event stream error: %s", err)
			}
			return err
		}

		if log.V(3) {
			log.Infof(ctx, "received events: %s", events.String())
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
