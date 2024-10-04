// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"net"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
)

type partitionedStreamClient struct {
	urlPlaceholder url.URL
	pgxConfig      *pgx.ConnConfig

	mu struct {
		syncutil.Mutex

		closed              bool
		activeSubscriptions map[*partitionedStreamSubscription]struct{}
		srcConn             *pgx.Conn // pgx connection to the source cluster
	}
}

func NewPartitionedStreamClient(
	ctx context.Context, remote *url.URL, opts ...Option,
) (Client, error) {
	options := processOptions(opts)
	config, err := setupPGXConfig(remote, options)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	client := partitionedStreamClient{
		urlPlaceholder: *remote,
		pgxConfig:      config,
	}
	client.mu.activeSubscriptions = make(map[*partitionedStreamSubscription]struct{})
	client.mu.srcConn = conn
	return &client, nil
}

var _ Client = &partitionedStreamClient{}

// Create implements Client interface.
func (p *partitionedStreamClient) Create(
	ctx context.Context, tenantName roachpb.TenantName, req streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	ctx, sp := tracing.ChildSpan(ctx, "streamclient.Client.Create")
	defer sp.Finish()
	p.mu.Lock()
	defer p.mu.Unlock()

	var row pgx.Row
	if !req.ReplicationStartTime.IsEmpty() {
		reqBytes, err := protoutil.Marshal(&req)
		if err != nil {
			return streampb.ReplicationProducerSpec{}, err
		}
		row = p.mu.srcConn.QueryRow(ctx, `SELECT crdb_internal.start_replication_stream($1, $2)`, tenantName, reqBytes)
	} else {
		row = p.mu.srcConn.QueryRow(ctx, `SELECT crdb_internal.start_replication_stream($1)`, tenantName)
	}

	var rawReplicationProducerSpec []byte
	err := row.Scan(&rawReplicationProducerSpec)
	if err != nil {
		return streampb.ReplicationProducerSpec{}, errors.Wrapf(err, "error creating replication stream for tenant %s", tenantName)
	}
	var replicationProducerSpec streampb.ReplicationProducerSpec
	if err := protoutil.Unmarshal(rawReplicationProducerSpec, &replicationProducerSpec); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}

	return replicationProducerSpec, err
}

// Dial implements Client interface.
func (p *partitionedStreamClient) Dial(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.mu.srcConn.Ping(ctx)
	return errors.Wrap(err, "failed to dial client")
}

// Heartbeat implements Client interface.
func (p *partitionedStreamClient) Heartbeat(
	ctx context.Context, streamID streampb.StreamID, consumed hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	ctx, sp := tracing.ChildSpan(ctx, "streamclient.Client.Heartbeat")
	defer sp.Finish()

	p.mu.Lock()
	defer p.mu.Unlock()
	row := p.mu.srcConn.QueryRow(ctx,
		`SELECT crdb_internal.replication_stream_progress($1, $2)`, streamID, consumed.String())
	var rawStatus []byte
	if err := row.Scan(&rawStatus); err != nil {
		return streampb.StreamReplicationStatus{},
			errors.Wrapf(err, "error sending heartbeat to replication stream %d", streamID)
	}
	var status streampb.StreamReplicationStatus
	if err := protoutil.Unmarshal(rawStatus, &status); err != nil {
		return streampb.StreamReplicationStatus{}, err
	}
	return status, nil
}

// postgresURL converts an SQL serving address into a postgres URL.
func (p *partitionedStreamClient) postgresURL(servingAddr string) (url.URL, error) {
	host, port, err := net.SplitHostPort(servingAddr)
	if err != nil {
		return url.URL{}, err
	}
	res := p.urlPlaceholder
	res.Host = net.JoinHostPort(host, port)
	return res, nil
}

// Plan implements Client interface.
func (p *partitionedStreamClient) Plan(
	ctx context.Context, streamID streampb.StreamID,
) (Topology, error) {
	var spec streampb.ReplicationStreamSpec
	{
		p.mu.Lock()
		defer p.mu.Unlock()

		row := p.mu.srcConn.QueryRow(ctx, `SELECT crdb_internal.replication_stream_spec($1)`, streamID)
		var rawSpec []byte
		if err := row.Scan(&rawSpec); err != nil {
			return Topology{}, errors.Wrapf(err, "error planning replication stream %d", streamID)
		}
		if err := protoutil.Unmarshal(rawSpec, &spec); err != nil {
			return Topology{}, err
		}
	}
	return p.createTopology(spec)
}

func (p *partitionedStreamClient) createTopology(
	spec streampb.ReplicationStreamSpec,
) (Topology, error) {
	topology := Topology{
		SourceTenantID: spec.SourceTenantID,
	}
	for _, sp := range spec.Partitions {
		pgURL, err := p.postgresURL(sp.SQLAddress.String())
		if err != nil {
			return Topology{}, err
		}
		rawSpec, err := protoutil.Marshal(sp.PartitionSpec)
		if err != nil {
			return Topology{}, err
		}
		topology.Partitions = append(topology.Partitions, PartitionInfo{
			ID:                sp.NodeID.String(),
			SubscriptionToken: SubscriptionToken(rawSpec),
			SrcInstanceID:     int(sp.NodeID),
			SrcAddr:           streamingccl.PartitionAddress(pgURL.String()),
			SrcLocality:       sp.Locality,
			Spans:             sp.PartitionSpec.Spans,
		})
	}
	return topology, nil
}

// Close implements Client interface.
func (p *partitionedStreamClient) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.closed {
		return nil
	}
	// Close all the active subscriptions and disallow more usage.
	p.mu.closed = true
	for sub := range p.mu.activeSubscriptions {
		close(sub.closeChan)
		delete(p.mu.activeSubscriptions, sub)
	}
	return p.mu.srcConn.Close(ctx)
}

// Subscribe implements Client interface.
func (p *partitionedStreamClient) Subscribe(
	ctx context.Context,
	streamID streampb.StreamID,
	spec SubscriptionToken,
	initialScanTime hlc.Timestamp,
	previousReplicatedTime hlc.Timestamp,
) (Subscription, error) {
	_, sp := tracing.ChildSpan(ctx, "streamclient.Client.Subscribe")
	defer sp.Finish()

	sps := streampb.StreamPartitionSpec{}
	if err := protoutil.Unmarshal(spec, &sps); err != nil {
		return nil, err
	}
	sps.InitialScanTimestamp = initialScanTime
	sps.PreviousReplicatedTimestamp = previousReplicatedTime

	specBytes, err := protoutil.Marshal(&sps)
	if err != nil {
		return nil, err
	}

	res := &partitionedStreamSubscription{
		eventsChan:    make(chan streamingccl.Event),
		srcConnConfig: p.pgxConfig,
		specBytes:     specBytes,
		streamID:      streamID,
		closeChan:     make(chan struct{}),
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.activeSubscriptions[res] = struct{}{}
	return res, nil
}

// Complete implements the streamclient.Client interface.
func (p *partitionedStreamClient) Complete(
	ctx context.Context, streamID streampb.StreamID, successfulIngestion bool,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "streamclient.Client.Complete")
	defer sp.Finish()

	p.mu.Lock()
	defer p.mu.Unlock()
	row := p.mu.srcConn.QueryRow(ctx,
		`SELECT crdb_internal.complete_replication_stream($1, $2)`, streamID, successfulIngestion)
	if err := row.Scan(&streamID); err != nil {
		return errors.Wrapf(err, "error completing replication stream %d", streamID)
	}
	return nil
}

type partitionedStreamSubscription struct {
	err           error
	srcConnConfig *pgx.ConnConfig
	eventsChan    chan streamingccl.Event
	// Channel to send signal to close the subscription.
	closeChan chan struct{}

	specBytes []byte
	streamID  streampb.StreamID
}

var _ Subscription = (*partitionedStreamSubscription)(nil)

// Subscribe implements the Subscription interface.
func (p *partitionedStreamSubscription) Subscribe(ctx context.Context) error {
	ctx, sp := tracing.ChildSpan(ctx, "partitionedStreamSubscription.Subscribe")
	defer sp.Finish()

	defer close(p.eventsChan)
	// Each subscription has its own pgx connection.
	srcConn, err := pgx.ConnectConfig(ctx, p.srcConnConfig)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			log.Warningf(ctx, "error when closing subscription connection: %v", err)
		}
	}()

	_, err = srcConn.Exec(ctx, `SET avoid_buffering = true`)
	if err != nil {
		return err
	}
	rows, err := srcConn.Query(ctx, `SELECT * FROM crdb_internal.stream_partition($1, $2)`,
		p.streamID, p.specBytes)
	if err != nil {
		return err
	}
	defer rows.Close()

	p.err = subscribeInternal(ctx, rows, p.eventsChan, p.closeChan)
	return p.err
}

// Events implements the Subscription interface.
func (p *partitionedStreamSubscription) Events() <-chan streamingccl.Event {
	return p.eventsChan
}

// Err implements the Subscription interface.
func (p *partitionedStreamSubscription) Err() error {
	return p.err
}
