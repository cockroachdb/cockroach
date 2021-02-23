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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
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
	ctx context.Context, pa streamingccl.PartitionAddress, startTime time.Time,
) (chan streamingccl.Event, error) {
	eventCh := make(chan streamingccl.Event)

	pgURL, err := pa.URL()
	if err != nil {
		return nil, err
	}
	q := pgURL.Query()
	tenantToReplicate := q.Get(TenantID)
	if len(tenantToReplicate) == 0 {
		return nil, errors.New("no tenant specified")
	}
	tenantID, err := strconv.Atoi(tenantToReplicate)
	if err != nil {
		return nil, errors.Wrap(err, "parsing tenant")
	}

	streamTenantQuery := fmt.Sprintf(
		`CREATE REPLICATION STREAM FOR TENANT %d WITH cursor='%d'`, tenantID, startTime.UnixNano())

	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConnectionString(pgURL.String())
	if err != nil {
		return nil, err
	}

	conn, err := pgx.Connect(pgxConfig)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryEx(ctx, streamTenantQuery, nil /* options */)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(eventCh)
		defer conn.Close()
		defer rows.Close()
		for rows.Next() {
			var ignoreTopic gosql.NullString
			var k, v []byte
			if err := rows.Scan(&ignoreTopic, &k, &v); err != nil {
				// Put it on error channel when rebased.
				panic(err)
			}

			var event streamingccl.Event
			if len(k) == 0 {
				var resolved hlc.Timestamp
				if err := protoutil.Unmarshal(v, &resolved); err != nil {
					// Put it on the error channel when rebased.
					panic(err)
				}
				event = streamingccl.MakeCheckpointEvent(resolved)
			} else {
				var kv roachpb.KeyValue
				kv.Key = k
				if err := protoutil.Unmarshal(v, &kv.Value); err != nil {
					// Put it on the error channel when rebased.
					panic(err)
				}
				event = streamingccl.MakeKVEvent(kv)
			}
			select {
			case eventCh <- event:
			case <-ctx.Done():
				// Put ctx.Err() on the err channel.
				panic("context cancel")
				return
			}
		}
	}()

	return eventCh, nil
}
