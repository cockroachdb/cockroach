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
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// sinklessReplicationClient creates and reads a stream from the source cluster.
type sinklessReplicationClient struct{}

var _ Client = &sinklessReplicationClient{}

// GetTopology implements the Client interface.
func (m *sinklessReplicationClient) GetTopology(
	sa streamingccl.StreamAddress,
) (streamingccl.Topology, error) {
	// The core changefeed clients only have 1 partition, and it's located at the
	// stream address.
	return streamingccl.Topology{
		Partitions: []streamingccl.PartitionAddress{streamingccl.PartitionAddress(sa)},
	}, nil
}

// ConsumePartition implements the Client interface.
func (m *sinklessReplicationClient) ConsumePartition(
	ctx context.Context, pa streamingccl.PartitionAddress, startTime hlc.Timestamp,
) (chan streamingccl.Event, chan error, error) {
	pgURL, err := pa.URL()
	if err != nil {
		return nil, nil, err
	}

	q := pgURL.Query()
	tenantToReplicate := q.Get(TenantID)
	if len(tenantToReplicate) == 0 {
		return nil, nil, errors.New("no tenant specified")
	}
	tenantID, err := strconv.Atoi(tenantToReplicate)
	if err != nil {
		return nil, nil, errors.Wrap(err, "parsing tenant")
	}

	streamTenantQuery := fmt.Sprintf(
		`CREATE REPLICATION STREAM FOR TENANT %d`, tenantID)
	if startTime.WallTime != 0 {
		streamTenantQuery = fmt.Sprintf(
			`CREATE REPLICATION STREAM FOR TENANT %d WITH cursor='%s'`, tenantID, startTime.AsOfSystemTime())
	}

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		return nil, nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}

	_, err = conn.ExecContext(ctx, `SET enable_experimental_stream_replication = true`)
	if err != nil {
		return nil, nil, err
	}
	rows, err := conn.QueryContext(ctx, streamTenantQuery)
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating source replication stream")
	}

	eventCh := make(chan streamingccl.Event)
	errCh := make(chan error, 1)

	go func() {
		defer close(eventCh)
		defer close(errCh)
		defer db.Close()
		defer rows.Close()
		for rows.Next() {
			var ignoreTopic gosql.NullString
			var k, v []byte
			if err := rows.Scan(&ignoreTopic, &k, &v); err != nil {
				errCh <- err
				return
			}

			var event streamingccl.Event
			if len(k) == 0 {
				var resolved hlc.Timestamp
				if err := protoutil.Unmarshal(v, &resolved); err != nil {
					errCh <- err
					return
				}
				event = streamingccl.MakeCheckpointEvent(resolved)
			} else {
				var kv roachpb.KeyValue
				kv.Key = k
				if err := protoutil.Unmarshal(v, &kv.Value); err != nil {
					errCh <- err
					return
				}
				event = streamingccl.MakeKVEvent(kv)
			}

			select {
			case eventCh <- event:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
		if err := rows.Err(); err != nil {
			errCh <- err
			return
		}
	}()

	return eventCh, errCh, nil
}
