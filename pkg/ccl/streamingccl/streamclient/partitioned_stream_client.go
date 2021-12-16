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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type partitionedStreamClient struct {
	// Connection the src cluster gateway
	remote *url.URL
}

var _ Client = &partitionedStreamClient{}

func (p *partitionedStreamClient) Create(
	ctx context.Context, tenantID roachpb.TenantID,
) (StreamID, error) {
	streamID := StreamID(streaming.InvalidStreamID)
	db, err := gosql.Open("postgres", p.remote.String())
	defer db.Close()
	if err != nil {
		return streamID, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return streamID, err
	}

	row := conn.QueryRowContext(ctx, `SELECT crdb_internal.start_replication_stream($1)`, tenantID.ToUint64())
	if row.Err() != nil {
		return streamID, errors.Wrapf(row.Err(), "Error in creating replication stream for tenant %s", tenantID.String())
	}

	err = row.Scan(&streamID)
	if err != nil {
		return streamID, err
	}
	return streamID, err
}

func (p *partitionedStreamClient) Heartbeat(
	ctx context.Context, ID StreamID, consumed hlc.Timestamp,
) error {
	db, err := gosql.Open("postgres", p.remote.String())
	defer db.Close()
	if err != nil {
		return err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	row := conn.QueryRowContext(ctx,
		`SELECT crdb_internal.replication_stream_progress($1, $2)`, ID, consumed.String())
	if row.Err() != nil {
		return errors.Wrapf(row.Err(), "Error in sending heartbeats to replication stream %d", ID)
	}

	var rawStatus []byte
	if err = row.Scan(&rawStatus); err != nil {
		return err
	}
	var status jobspb.StreamReplicationStatus
	if err := protoutil.Unmarshal(rawStatus, &status); err != nil {
		return err
	}
	// TODO(casper): add observability for stream protected timestamp
	return MakeReplicationStreamError(ID, status)
}

func (p *partitionedStreamClient) Plan(ctx context.Context, ID StreamID) (Topology, error) {
	db, err := gosql.Open("postgres", p.remote.String())
	defer db.Close()
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	row := conn.QueryRowContext(ctx, `SELECT crdb_internal.replication_stream_spec($1)`, ID)
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

	// we need to use the disqlplanner to find all the planner nodes and
	// assign partition to it in a round-robin fashion
	topology := Topology{}
	for _, p := range spec.Partitions {
		rawSpec, err := protoutil.Marshal(p.PartitionSpec)
		if err != nil {
			return nil, err
		}
		topology = append(topology, PartitionInfo{
			ID:                p.NodeID.String(), // how do we determine partition id?
			SubscriptionToken: SubscriptionToken(rawSpec),
			SrcInstanceID:     int(p.NodeID),
			SrcAddr:           streamingccl.PartitionAddress(p.SQLAddress.String()),
			SrcLocality:       p.Locality,
		})
	}
	return topology, nil
}

type partitionStreamDecoder struct {
	rows *gosql.Rows
	e    streampb.StreamEvent
}

func (d *partitionStreamDecoder) pop() streamingccl.Event {
	if d.e.Checkpoint != nil {
		event := streamingccl.MakeCheckpointEvent(d.e.Checkpoint.Spans[0].Timestamp)
		d.e.Checkpoint = nil
		return event
	}

	if d.e.Batch != nil {
		event := streamingccl.MakeKVEvent(d.e.Batch.KeyValues[0])
		d.e.Batch.KeyValues = d.e.Batch.KeyValues[1:]
		if len(d.e.Batch.KeyValues) == 0 {
			d.e.Batch = nil
		}
		return event
	}

	return nil
}

func (d *partitionStreamDecoder) decode() error {
	var data []byte
	if err := d.rows.Scan(&data); err != nil {
		return err
	}
	var streamEvent streampb.StreamEvent
	if streamEvent.Checkpoint == nil && streamEvent.Batch == nil {
		return errors.New("unexpected event type")
	}
	d.e = streamEvent
	return nil
}

func (p *partitionedStreamClient) Subscribe(
	ctx context.Context, stream StreamID, spec SubscriptionToken, checkpoint hlc.Timestamp,
) (chan streamingccl.Event, chan error, error) {

	db, err := gosql.Open("postgres", p.remote.String())
	if err != nil {
		return nil, nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}

	sps := streampb.StreamPartitionSpec{}
	if err = protoutil.Unmarshal(spec, &sps); err != nil {
		return nil, nil, err
	}
	sps.StartFrom = checkpoint

	specBytes, err := protoutil.Marshal(&sps)
	if err != nil {
		return nil, nil, err
	}

	rows, err := conn.QueryContext(ctx, `SELECT * FROM crdb_internal.stream_partition($1)`, specBytes)
	if err != nil {
		return nil, nil, err
	}

	eventChan := make(chan streamingccl.Event)
	errChan := make(chan error)
	// Spin up a goroutine to read rows in partition stream
	go func() {
		psd := partitionStreamDecoder{rows: rows}
		for rows.Next() {
			if err := psd.decode(); err != nil {
				errChan <- err
				break
			}
			for e := psd.pop(); e != nil; {
				eventChan <- e
			}
		}
		rows.Close()
		db.Close()
	}()
	return eventChan, errChan, nil
}
