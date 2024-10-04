// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

// SpanConfigClient provides methods to interact with a stream of span
// config updates of a specific application tenant.
type SpanConfigClient interface {
	Dialer

	// SetupSpanConfigsStream creates a stream for the span configs
	// that apply to the passed in tenant, and returns the subscriptions the
	// client can subscribe to. No protected timestamp or job is persisted to the
	// source cluster.
	SetupSpanConfigsStream(tenant roachpb.TenantName) (Subscription, error)
}
type spanConfigClient struct {
	pgxConfig    *pgx.ConnConfig
	srcConn      *pgx.Conn // pgx connection to the source cluster
	subscription spanConfigStreamSubscription
}

var _ SpanConfigClient = &spanConfigClient{}

func NewSpanConfigStreamClient(
	ctx context.Context, remote *url.URL, db isql.DB, opts ...Option,
) (SpanConfigClient, error) {

	if remote.Scheme == "external" {
		if db == nil {
			return nil, errors.AssertionFailedf("nil db handle can't be used to dereference external URI")
		}
		addr, err := lookupExternalConnection(ctx, remote.Host, db)
		if err != nil {
			return nil, err
		}
		url, err := addr.URL()
		if err != nil {
			return nil, err
		}
		return NewSpanConfigStreamClient(ctx, url, db, opts...)
	}

	options := processOptions(opts)
	config, err := setupPGXConfig(remote, options)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return &spanConfigClient{
		pgxConfig: config,
		srcConn:   conn,
	}, nil
}

// GetFirstActiveSpanConfigClient iterates through each provided stream address
// and returns the first client it's able to successfully Dial.
func GetFirstActiveSpanConfigClient(
	ctx context.Context, streamAddresses []string, db isql.DB, opts ...Option,
) (SpanConfigClient, error) {

	newClient := func(ctx context.Context, address streamingccl.StreamAddress) (Dialer, error) {
		streamURL, err := address.URL()
		if err != nil {
			return nil, err
		}
		return NewSpanConfigStreamClient(ctx, streamURL, db, opts...)
	}
	dialer, err := getFirstDialer(ctx, streamAddresses, newClient)
	if err != nil {
		return nil, err
	}
	return dialer.(SpanConfigClient), err
}

// Dial implements Client interface.
func (p *spanConfigClient) Dial(ctx context.Context) error {
	err := p.srcConn.Ping(ctx)
	return errors.Wrap(err, "failed to dial client")
}

func (p *spanConfigClient) Close(ctx context.Context) error {
	close(p.subscription.closeChan)
	return p.srcConn.Close(ctx)
}

func (p *spanConfigClient) SetupSpanConfigsStream(tenant roachpb.TenantName) (Subscription, error) {
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
	ctx, sp := tracing.ChildSpan(ctx, "spanConfigStreamSubscription.Subscribe")
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
		// TODO(adityamaru): This is a short term fix for https://github.com/cockroachdb/cockroach/issues/113682
		// to allow for a < 23.2 source cluster to replicate into a 23.2 destination
		// cluster. In the long term we will want to use the source cluster's
		// cluster version to gate features on the destination.
		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
			if pgcode.MakeCode(pgErr.Code) == pgcode.UndefinedFunction {
				log.Warningf(ctx, "source cluster is running a version < 23.2, skipping span config replication: %v", err)
				return nil
			}
		}
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
