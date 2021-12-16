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
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// sinklessReplicationClient creates and reads a stream from the source cluster.
type sinklessReplicationClient struct {
	remote *url.URL
}

var _ Client = &sinklessReplicationClient{}

// newPGWireReplicationClient returns a stream client that interacts with a
// remote cluster over a pgwire connection.
func newPGWireReplicationClient(remote *url.URL) (Client, error) {
	return &sinklessReplicationClient{remote: remote}, nil
}

// Create implements the Client interface.
func (m *sinklessReplicationClient) Create(
	ctx context.Context, tenantID roachpb.TenantID,
) (streaming.StreamID, error) {
	return streaming.StreamID(tenantID.ToUint64()), nil
}

// Heartbeat implements the Client interface.
func (m *sinklessReplicationClient) Heartbeat(
	ctx context.Context, streamID streaming.StreamID, complete hlc.Timestamp,
) error {
	return nil
}

// Plan implements the Client interface.
func (m *sinklessReplicationClient) Plan(
	ctx context.Context, ID streaming.StreamID,
) (Topology, error) {
	// The core changefeed clients only have 1 partition, and it's located at the
	// stream address.
	return Topology([]PartitionInfo{
		{
			ID:                "1",
			SrcAddr:           streamingccl.PartitionAddress(m.remote.String()),
			SubscriptionToken: []byte(strconv.Itoa(int(ID))),
		},
	}), nil
}

// Close implements the Client interface.
func (m *sinklessReplicationClient) Close() error {
	return nil
}

// Subscribe implements the Client interface.
func (m *sinklessReplicationClient) Subscribe(
	ctx context.Context, stream streaming.StreamID, spec SubscriptionToken, checkpoint hlc.Timestamp,
) (Subscription, error) {
	tenantToReplicate := string(spec)
	tenantID, err := strconv.Atoi(tenantToReplicate)
	if err != nil {
		return nil, errors.Wrap(err, "parsing tenant")
	}

	streamTenantQuery := fmt.Sprintf(
		`CREATE REPLICATION STREAM FOR TENANT %d`, tenantID)
	if checkpoint.WallTime != 0 {
		streamTenantQuery = fmt.Sprintf(
			`CREATE REPLICATION STREAM FOR TENANT %d WITH cursor='%s'`, tenantID, checkpoint.AsOfSystemTime())
	}

	db, err := gosql.Open("postgres", m.remote.String())
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	_, err = conn.ExecContext(ctx, `SET enable_experimental_stream_replication = true`)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, streamTenantQuery)
	if err != nil {
		return nil, errors.Wrap(err, "creating source replication stream")
	}

	sub := &sinklessReplicationSubscription{eventCh: make(chan streamingccl.Event)}
	sub.receiveFn = func(ctx context.Context) error {
		defer close(sub.eventCh)
		defer db.Close()
		defer rows.Close()
		for rows.Next() {
			var ignoreTopic gosql.NullString
			var k, v []byte
			if err := rows.Scan(&ignoreTopic, &k, &v); err != nil {
				sub.err = err
				return err
			}

			var event streamingccl.Event
			if len(k) == 0 {
				var resolved hlc.Timestamp
				if err := protoutil.Unmarshal(v, &resolved); err != nil {
					sub.err = err
					return err
				}
				event = streamingccl.MakeCheckpointEvent(resolved)
			} else {
				var kv roachpb.KeyValue
				kv.Key = k
				if err := protoutil.Unmarshal(v, &kv.Value); err != nil {
					sub.err = err
					return err
				}
				event = streamingccl.MakeKVEvent(kv)
			}

			select {
			case sub.eventCh <- event:
			case <-ctx.Done():
				sub.err = err
				return ctx.Err()
			}
		}
		if err := rows.Err(); err != nil {
			if errors.Is(err, driver.ErrBadConn) {
				select {
				case sub.eventCh <- streamingccl.MakeGenerationEvent():
				case <-ctx.Done():
					sub.err = ctx.Err()
				}
			} else {
				sub.err = err
			}
			return err
		}
		return nil
	}

	return sub, nil
}

type sinklessReplicationSubscription struct {
	eventCh   chan streamingccl.Event
	err       error
	receiveFn func(ctx context.Context) error
}

// Receive implements Subscription interface.
func (s *sinklessReplicationSubscription) Receive(ctx context.Context) error {
	return s.receiveFn(ctx)
}

// Events implements Subscription interface.
func (s *sinklessReplicationSubscription) Events() <-chan streamingccl.Event {
	return s.eventCh
}

// Err implements Subscription interface.
func (s *sinklessReplicationSubscription) Err() error {
	return s.err
}
