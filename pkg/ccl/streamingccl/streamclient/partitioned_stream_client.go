// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	gosql "database/sql"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type partitionedStreamClient struct {
	db *gosql.DB // DB handle to the source cluster
}

func makePartitionedStreamClient(remote *url.URL) (*partitionedStreamClient, error) {
	db, err := gosql.Open("postgres", remote.String())
	if err != nil {
		return nil, err
	}
	return &partitionedStreamClient{db: db}, nil
}

var _ Client = &partitionedStreamClient{}

// Create implements Client interface.
func (p *partitionedStreamClient) Create(
	ctx context.Context, tenantID roachpb.TenantID,
) (streaming.StreamID, error) {
	streamID := streaming.InvalidStreamID

	conn, err := p.db.Conn(ctx)
	if err != nil {
		return streamID, err
	}
	defer conn.Close()

	row := conn.QueryRowContext(ctx, `SELECT crdb_internal.start_replication_stream($1)`, tenantID.ToUint64())
	if row.Err() != nil {
		return streamID, errors.Wrapf(row.Err(), "Error in creating replication stream for tenant %s", tenantID.String())
	}

	err = row.Scan(&streamID)
	return streamID, err
}

// Heartbeat implements Client interface.
func (p *partitionedStreamClient) Heartbeat(
	ctx context.Context, streamID streaming.StreamID, consumed hlc.Timestamp,
) error {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	row := conn.QueryRowContext(ctx,
		`SELECT crdb_internal.replication_stream_progress($1, $2)`, streamID, consumed.String())
	if row.Err() != nil {
		return errors.Wrapf(row.Err(), "Error in sending heartbeats to replication stream %d", streamID)
	}

	var rawStatus []byte
	if err := row.Scan(&rawStatus); err != nil {
		return err
	}
	var status streampb.StreamReplicationStatus
	if err := protoutil.Unmarshal(rawStatus, &status); err != nil {
		return err
	}
	// TODO(casper): add observability for stream protected timestamp
	if status.StreamStatus != streampb.StreamReplicationStatus_STREAM_ACTIVE {
		return errors.Errorf("Replication stream %d is not running, status is %s", streamID, status.StreamStatus.String())
	}
	return nil
}

// Plan implements Client interface.
func (p *partitionedStreamClient) Plan(
	ctx context.Context, streamID streaming.StreamID,
) (Topology, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	row := conn.QueryRowContext(ctx, `SELECT crdb_internal.replication_stream_spec($1)`, streamID)
	if row.Err() != nil {
		return nil, errors.Wrap(row.Err(), "Error in planning a replication stream")
	}

	var rawSpec []byte
	if err = row.Scan(&rawSpec); err != nil {
		return nil, err
	}
	var spec streampb.ReplicationStreamSpec
	if err := protoutil.Unmarshal(rawSpec, &spec); err != nil {
		return nil, err
	}

	topology := Topology{}
	for _, p := range spec.Partitions {
		rawSpec, err := protoutil.Marshal(p.PartitionSpec)
		if err != nil {
			return nil, err
		}
		topology = append(topology, PartitionInfo{
			ID:                p.NodeID.String(), // how do determine partition ID?
			SubscriptionToken: SubscriptionToken(rawSpec),
			SrcInstanceID:     int(p.NodeID),
			SrcAddr:           streamingccl.PartitionAddress(p.SQLAddress.String()),
			SrcLocality:       p.Locality,
		})
	}
	return topology, nil
}

// Close implements Client interface.
func (p *partitionedStreamClient) Close() error {
	return p.db.Close()
}

// Subscribe implements Client interface.
func (p *partitionedStreamClient) Subscribe(
	ctx context.Context, stream streaming.StreamID, spec SubscriptionToken, checkpoint hlc.Timestamp,
) (Subscription, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	sps := streampb.StreamPartitionSpec{}
	if err = protoutil.Unmarshal(spec, &sps); err != nil {
		return nil, err
	}
	sps.StartFrom = checkpoint

	specBytes, err := protoutil.Marshal(&sps)
	if err != nil {
		return nil, err
	}

	return &partitionedStreamSubscription{
		eventsChan: make(chan streamingccl.Event),
		errChan:    make(chan error, 1),
		db:         p.db,
		specBytes:  specBytes,
		streamID:   stream,
	}, nil
}

type partitionedStreamSubscription struct {
	eventsChan chan streamingccl.Event
	errChan    chan error
	db *gosql.DB

	streamEvent streampb.StreamEvent
	specBytes []byte
	streamID streaming.StreamID
}

// Receive implements Subscription interface.
func (p *partitionedStreamSubscription) Receive(ctx context.Context) error {
	defer close(p.eventsChan)
	defer close(p.errChan)
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, `SET avoid_buffering = true`)
	if err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, `SELECT * FROM crdb_internal.stream_partition($1, $2)`,
		p.streamID, p.specBytes)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Pop event from the last batch of events.
	popEvent := func() streamingccl.Event {
		if p.streamEvent.Checkpoint != nil {
			event := streamingccl.MakeCheckpointEvent(p.streamEvent.Checkpoint.Spans[0].Timestamp)
			p.streamEvent.Checkpoint = nil
			return event
		}
		if p.streamEvent.Batch != nil {
			event := streamingccl.MakeKVEvent(p.streamEvent.Batch.KeyValues[0])
			p.streamEvent.Batch.KeyValues = p.streamEvent.Batch.KeyValues[1:]
			if len(p.streamEvent.Batch.KeyValues) == 0 {
				p.streamEvent.Batch = nil
			}
			return event
		}
		return nil
	}

	// Get the next event from the cursor.
	getNextEvent := func() (streamingccl.Event, error) {
		if e := popEvent(); e != nil {
			return e, nil
		}

		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var streamEvent streampb.StreamEvent
		if err := protoutil.Unmarshal(data, &streamEvent); err != nil {
			return nil, err
		}
		p.streamEvent = streamEvent
		return popEvent(), nil
	}

	for {
		event, err := getNextEvent()
		if err != nil {
			p.errChan <- ctx.Err()
			return err
		}
		select {
		case p.eventsChan <- event:
		case <-ctx.Done():
			p.errChan <- ctx.Err()
			return ctx.Err()
		}
	}
}

// Events implements Subscription interface.
func (p *partitionedStreamSubscription) Events() <-chan streamingccl.Event {
	return p.eventsChan
}

// Err implements Subscription interface.
func (p *partitionedStreamSubscription) Err() <-chan error {
	return p.errChan
}

