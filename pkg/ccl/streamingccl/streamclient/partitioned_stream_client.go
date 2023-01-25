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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
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
	ctx context.Context, remote *url.URL,
) (*partitionedStreamClient, error) {

	noInlineCertURI, tlsInfo, err := uriWithInlineTLSCertsRemoved(remote)
	if err != nil {
		return nil, err
	}
	config, err := pgx.ParseConfig(noInlineCertURI.String())
	if err != nil {
		return nil, err
	}
	tlsInfo.addTLSCertsToConfig(config.TLSConfig)

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	client := &partitionedStreamClient{
		urlPlaceholder: *remote,
		pgxConfig:      config,
	}
	client.mu.activeSubscriptions = make(map[*partitionedStreamSubscription]struct{})
	client.mu.srcConn = conn
	return client, nil
}

var _ Client = &partitionedStreamClient{}

// Create implements Client interface.
func (p *partitionedStreamClient) Create(
	ctx context.Context, tenantName roachpb.TenantName,
) (streampb.ReplicationProducerSpec, error) {
	ctx, sp := tracing.ChildSpan(ctx, "streamclient.Client.Create")
	defer sp.Finish()

	p.mu.Lock()
	defer p.mu.Unlock()
	var rawReplicationProducerSpec []byte
	row := p.mu.srcConn.QueryRow(ctx, `SELECT crdb_internal.start_replication_stream($1)`, tenantName)
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
	previousHighWater hlc.Timestamp,
) (Subscription, error) {
	_, sp := tracing.ChildSpan(ctx, "streamclient.Client.Subscribe")
	defer sp.Finish()

	sps := streampb.StreamPartitionSpec{}
	if err := protoutil.Unmarshal(spec, &sps); err != nil {
		return nil, err
	}
	sps.InitialScanTimestamp = initialScanTime
	sps.PreviousHighWaterTimestamp = previousHighWater

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

	streamEvent *streampb.StreamEvent
	specBytes   []byte
	streamID    streampb.StreamID
}

var _ Subscription = (*partitionedStreamSubscription)(nil)

// parseEvent parses next event from the batch of events inside streampb.StreamEvent.
func parseEvent(streamEvent *streampb.StreamEvent) streamingccl.Event {
	if streamEvent == nil {
		return nil
	}

	if streamEvent.Checkpoint != nil {
		event := streamingccl.MakeCheckpointEvent(streamEvent.Checkpoint.ResolvedSpans)
		streamEvent.Checkpoint = nil
		return event
	}

	var event streamingccl.Event
	if streamEvent.Batch != nil {
		if len(streamEvent.Batch.Ssts) > 0 {
			event = streamingccl.MakeSSTableEvent(streamEvent.Batch.Ssts[0])
			streamEvent.Batch.Ssts = streamEvent.Batch.Ssts[1:]
		} else if len(streamEvent.Batch.KeyValues) > 0 {
			event = streamingccl.MakeKVEvent(streamEvent.Batch.KeyValues[0])
			streamEvent.Batch.KeyValues = streamEvent.Batch.KeyValues[1:]
		} else if len(streamEvent.Batch.DelRanges) > 0 {
			event = streamingccl.MakeDeleteRangeEvent(streamEvent.Batch.DelRanges[0])
			streamEvent.Batch.DelRanges = streamEvent.Batch.DelRanges[1:]
		}
		if len(streamEvent.Batch.KeyValues) == 0 &&
			len(streamEvent.Batch.Ssts) == 0 &&
			len(streamEvent.Batch.DelRanges) == 0 {
			streamEvent.Batch = nil
		}
	}
	return event
}

// Subscribe implements the Subscription interface.
func (p *partitionedStreamSubscription) Subscribe(ctx context.Context) error {
	ctx, sp := tracing.ChildSpan(ctx, "Subscription.Subscribe")
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

	// Get the next event from the cursor.
	getNextEvent := func() (streamingccl.Event, error) {
		if e := parseEvent(p.streamEvent); e != nil {
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
		p.streamEvent = &streamEvent
		return parseEvent(p.streamEvent), nil
	}

	for {
		event, err := getNextEvent()
		if err != nil {
			p.err = err
			return err
		}
		select {
		case p.eventsChan <- event:
		case <-p.closeChan:
			// Exit quietly to not cause other subscriptions in the same
			// ctxgroup.Group to exit.
			return nil
		case <-ctx.Done():
			p.err = ctx.Err()
			return p.err
		}
	}
}

// Events implements the Subscription interface.
func (p *partitionedStreamSubscription) Events() <-chan streamingccl.Event {
	return p.eventsChan
}

// Err implements the Subscription interface.
func (p *partitionedStreamSubscription) Err() error {
	return p.err
}

type tlsCerts struct {
	certs        []tls.Certificate
	rootCertPool *x509.CertPool
}

const (
	// sslInlineURLParam is a non-standard connection URL
	// parameter. When true, we assume that sslcert, sslkey, and
	// sslrootcert contain URL-encoded data rather than paths.
	sslInlineURLParam = "sslinline"

	sslModeURLParam     = "sslmode"
	sslCertURLParam     = "sslcert"
	sslKeyURLParam      = "sslkey"
	sslRootCertURLParam = "sslrootcert"
)

var RedactableURLParameters = []string{
	sslCertURLParam,
	sslKeyURLParam,
	sslRootCertURLParam,
}

// uriWithInlineTLSCertsRemoved handles the non-standard sslinline
// option. The returned URL can be passed to pgx. The returned
// tlsCerts struct can be used to apply the certificate data to the
// tls.Config produced by pgx.
func uriWithInlineTLSCertsRemoved(remote *url.URL) (*url.URL, *tlsCerts, error) {
	if remote.Query().Get(sslInlineURLParam) != "true" {
		return remote, nil, nil
	}

	retURL := *remote
	v := retURL.Query()
	cert := v.Get(sslCertURLParam)
	key := v.Get(sslKeyURLParam)
	rootcert := v.Get(sslRootCertURLParam)

	if (cert != "" && key == "") || (cert == "" && key != "") {
		return nil, nil, errors.New(`both "sslcert" and "sslkey" are required`)
	}

	tlsInfo := &tlsCerts{}
	if rootcert != "" {
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM([]byte(rootcert)) {
			return nil, nil, errors.New("unable to add CA to cert pool")
		}
		tlsInfo.rootCertPool = caCertPool
	}
	if cert != "" && key != "" {
		// TODO(ssd): pgx supports sslpassword here. But, it
		// only supports PKCS#1 and relies on functions that
		// are deprecated in the stdlib. For now, I've skipped
		// it.
		block, _ := pem.Decode([]byte(key))
		pemKey := pem.EncodeToMemory(block)
		keyPair, err := tls.X509KeyPair([]byte(cert), pemKey)
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to construct x509 key pair")
		}
		tlsInfo.certs = []tls.Certificate{keyPair}
	}

	// lib/pq, pgx, and the C libpq implement this backwards
	// compatibility quirk. Since we are removing sslrootcert, we
	// have to re-implement it here.
	//
	// TODO(ssd): This may be a sign that we should implement the
	// entire configTLS function from pgx and remove all tls
	// options.
	if v.Get(sslModeURLParam) == "require" && rootcert != "" {
		v.Set(sslModeURLParam, "verify-ca")
	}

	v.Del(sslCertURLParam)
	v.Del(sslKeyURLParam)
	v.Del(sslRootCertURLParam)
	v.Del(sslInlineURLParam)
	retURL.RawQuery = v.Encode()
	return &retURL, tlsInfo, nil
}

func (c *tlsCerts) addTLSCertsToConfig(tlsConfig *tls.Config) {
	if c == nil {
		return
	}

	if c.rootCertPool != nil {
		tlsConfig.RootCAs = c.rootCertPool
		tlsConfig.ClientCAs = c.rootCertPool
	}

	if len(c.certs) > 0 {
		tlsConfig.Certificates = c.certs
	}
}
