// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
)

type spanConfigStreamClient struct {
	pgxConfig    *pgx.ConnConfig
	srcConn      *pgx.Conn // pgx connection to the source cluster
	subscription spanConfigStreamSubscription
}

// TODO(msbutler): once the span config stream is hooked up on the ingestion
// side, consider implementing a different interface for the spanConfig client.
var _ Client = &spanConfigStreamClient{}

func NewSpanConfigStreamClient(
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
	return &spanConfigStreamClient{
		pgxConfig: config,
		srcConn:   conn,
	}, nil
}

func (m *spanConfigStreamClient) Create(
	_ context.Context, _ roachpb.TenantName,
) (streampb.ReplicationProducerSpec, error) {
	return streampb.ReplicationProducerSpec{}, errors.New("spanConfigStreamClient cannot create replication producer job")
}

func (m *spanConfigStreamClient) Plan(_ context.Context, _ streampb.StreamID) (Topology, error) {
	return Topology{}, errors.New("spanConfigStream cannot cannot create a distributed replication stream plan")
}

func (m *spanConfigStreamClient) Heartbeat(
	_ context.Context, _ streampb.StreamID, _ hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	return streampb.StreamReplicationStatus{}, errors.New("spanConfigStreamClient does not create a producer job, so it does not implement Heartbeat()")
}

// Dial implements Client interface.
func (p *spanConfigStreamClient) Dial(ctx context.Context) error {
	err := p.srcConn.Ping(ctx)
	return errors.Wrap(err, "failed to dial client")
}

// Subscribe implements the Subscription interface.
func (m *spanConfigStreamClient) Subscribe(
	_ context.Context, _ streampb.StreamID, _ SubscriptionToken, _ hlc.Timestamp, _ hlc.Timestamp,
) (Subscription, error) {
	return nil, errors.New("spanConfigStreamClient creates a subscription via SetupSPanConfigStream")
}

func (p *spanConfigStreamClient) Complete(
	ctx context.Context, streamID streampb.StreamID, successfulIngestion bool,
) error {
	return errors.New("spanConfigStreamClient does not create a producer job, so it does implement Complete()")
}

func (p *spanConfigStreamClient) Close(ctx context.Context) error {
	close(p.subscription.closeChan)
	return p.srcConn.Close(ctx)
}

func (p *spanConfigStreamClient) SetupSpanConfigsStream(
	ctx context.Context, tenant roachpb.TenantName,
) (Subscription, error) {
	p.subscription = spanConfigStreamSubscription{
		eventsChan:    make(chan streamingccl.Event),
		srcConnConfig: p.pgxConfig,
		tenantName:    tenant,
		closeChan:     make(chan struct{}),
	}

	return &p.subscription, nil
}

type spanConfigStreamSubscription struct {
	err           error
	srcConnConfig *pgx.ConnConfig
	eventsChan    chan streamingccl.Event
	tenantName    roachpb.TenantName
	// Channel to send signal to close the subscription.
	closeChan chan struct{}
}

var _ Subscription = (*spanConfigStreamSubscription)(nil)

// Subscribe implements the Subscription interface.
func (p *spanConfigStreamSubscription) Subscribe(ctx context.Context) error {
	ctx, sp := tracing.ChildSpan(ctx, "Subscription.Subscribe")
	defer sp.Finish()

	defer close(p.eventsChan)
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

	cancelCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	rows, err := srcConn.Query(cancelCtx, `SELECT crdb_internal.setup_span_configs_stream($1)`, p.tenantName)
	if err != nil {
		return err
	}
	// For close to return, the ctx passed to the query above must be cancelled.
	//
	// TODO (msbutler): clean up how the implementations of
	// subscription.Subscribe() return. It seems both the spanConfig and
	// partitioned implementations require their contexts to be cancelled before
	// returning, which is quite unfortunate.
	defer func() {
		cancelFunc()
		rows.Close()
	}()

	p.err = subscribeInternal(ctx, rows, p.eventsChan, p.closeChan)
	return p.err
}

// Events implements the Subscription interface.
func (p *spanConfigStreamSubscription) Events() <-chan streamingccl.Event {
	return p.eventsChan
}

// Err implements the Subscription interface.
func (p *spanConfigStreamSubscription) Err() error {
	return p.err
}
