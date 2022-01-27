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
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type partitionedStreamClient struct {
	db *gosql.DB // DB handle to the source cluster

	mu struct {
		syncutil.Mutex

		closed              bool
		pgURLCertCleanup    []func()
		activeSubscriptions map[*partitionedStreamSubscription]struct{}
	}
}

func newPartitionedStreamClient(remote *url.URL) (*partitionedStreamClient, error) {
	db, err := gosql.Open("postgres", remote.String())
	if err != nil {
		return nil, err
	}
	client := &partitionedStreamClient{db: db}
	client.mu.activeSubscriptions = make(map[*partitionedStreamSubscription]struct{})
	return client, nil
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
	defer func() {
		_ = conn.Close()
	}()

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
	defer func() {
		_ = conn.Close()
	}()

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

// postgresURL converts an SQL serving address into a postgres URL. In order to connect securely
// using postgres, this method will create temporary on-disk copies of
// certain embedded security certificates. The certificates will be created in a new temporary
// directory. The returned cleanup function will delete this temporary directory.
func postgresURL(servingAddr string, user *url.Userinfo) (url.URL, func(), error) {
	host, port, err := net.SplitHostPort(servingAddr)
	if err != nil {
		return url.URL{}, func() {}, err
	}

	tempDir, err := ioutil.TempDir("", fileutil.EscapeFilename("StreamClientPGConnection"))
	if err != nil {
		return url.URL{}, func() {}, err
	}

	// This CA is the one used by the SQL client driver to authenticate KV nodes on the host cluster.
	caPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	tempCAPath, err := securitytest.RestrictedCopy(caPath, tempDir, "ca")

	if err != nil {
		return url.URL{}, func() {}, err
	}
	// This CA is the one used by the SQL client driver to authenticate SQL tenant servers.
	tenantCAPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedTenantCACert)
	if err := securitytest.AppendFile(tenantCAPath, tempCAPath); err != nil {
		return url.URL{}, func() {}, err
	}
	options := url.Values{}
	options.Add("sslrootcert", tempCAPath)

	certPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.crt", user.Username()))
	keyPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.key", user.Username()))

	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	tempCertPath, err := securitytest.RestrictedCopy(certPath, tempDir, "cert")
	if err != nil {
		return url.URL{}, func() {}, err
	}
	tempKeyPath, err := securitytest.RestrictedCopy(keyPath, tempDir, "key")
	if err != nil {
		return url.URL{}, func() {}, err
	}
	options.Add("sslcert", tempCertPath)
	options.Add("sslkey", tempKeyPath)
	options.Add("sslmode", "verify-full")

	return url.URL{
		Scheme:   "postgres",
		User:     user,
		Host:     net.JoinHostPort(host, port),
		RawQuery: options.Encode(),
	}, func() { _ = os.RemoveAll(tempDir) }, nil
}

// Plan implements Client interface.
func (p *partitionedStreamClient) Plan(
	ctx context.Context, streamID streaming.StreamID,
) (Topology, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	row := conn.QueryRowContext(ctx, `SELECT crdb_internal.replication_stream_spec($1)`, streamID)
	if row.Err() != nil {
		return nil, errors.Wrap(row.Err(), "Error in planning a replication stream")
	}

	var rawSpec []byte
	if err := row.Scan(&rawSpec); err != nil {
		return nil, err
	}
	var spec streampb.ReplicationStreamSpec
	if err := protoutil.Unmarshal(rawSpec, &spec); err != nil {
		return nil, err
	}

	topology := Topology{}
	for _, sp := range spec.Partitions {
		pgURL, cleanupCertDir, err := postgresURL(sp.SQLAddress.String(), url.User(security.RootUser))
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.mu.pgURLCertCleanup = append(p.mu.pgURLCertCleanup, cleanupCertDir)
		p.mu.Unlock()
		rawSpec, err := protoutil.Marshal(sp.PartitionSpec)
		if err != nil {
			return nil, err
		}
		topology = append(topology, PartitionInfo{
			ID:                sp.NodeID.String(),
			SubscriptionToken: SubscriptionToken(rawSpec),
			SrcInstanceID:     int(sp.NodeID),
			SrcAddr:           streamingccl.PartitionAddress(pgURL.String()),
			SrcLocality:       sp.Locality,
		})
	}
	return topology, nil
}

// Close implements Client interface.
func (p *partitionedStreamClient) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.closed {
		return nil
	}
	// Close all the active subscriptions and disallow more usage.
	p.mu.closed = true
	for sub := range p.mu.activeSubscriptions {
		sub.Close()
		delete(p.mu.activeSubscriptions, sub)
	}
	for _, cleanup := range p.mu.pgURLCertCleanup {
		cleanup()
	}
	return p.db.Close()
}

// Subscribe implements Client interface.
func (p *partitionedStreamClient) Subscribe(
	ctx context.Context, stream streaming.StreamID, spec SubscriptionToken, checkpoint hlc.Timestamp,
) (Subscription, error) {

	sps := streampb.StreamPartitionSpec{}
	if err := protoutil.Unmarshal(spec, &sps); err != nil {
		return nil, err
	}
	sps.StartFrom = checkpoint

	specBytes, err := protoutil.Marshal(&sps)
	if err != nil {
		return nil, err
	}

	res := &partitionedStreamSubscription{
		eventsChan: make(chan streamingccl.Event),
		db:         p.db,
		specBytes:  specBytes,
		streamID:   stream,
		// Buffered channel allows caller to close an already closed subscription
		// without causing deadlock.
		closeChan: make(chan struct{}, 10),
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.activeSubscriptions[res] = struct{}{}
	return res, nil
}

type partitionedStreamSubscription struct {
	err        error
	db         *gosql.DB
	eventsChan chan streamingccl.Event
	closeChan  chan struct{}
	closed     bool

	streamEvent *streampb.StreamEvent
	specBytes   []byte
	streamID    streaming.StreamID
}

var _ Subscription = (*partitionedStreamSubscription)(nil)

// parseEvent parses next event from the batch of events inside streampb.StreamEvent.
func parseEvent(streamEvent *streampb.StreamEvent) streamingccl.Event {
	if streamEvent == nil {
		return nil
	}

	if streamEvent.Checkpoint != nil {
		event := streamingccl.MakeCheckpointEvent(streamEvent.Checkpoint.Spans[0].Timestamp)
		streamEvent.Checkpoint = nil
		return event
	}
	if streamEvent.Batch != nil {
		event := streamingccl.MakeKVEvent(streamEvent.Batch.KeyValues[0])
		streamEvent.Batch.KeyValues = streamEvent.Batch.KeyValues[1:]
		if len(streamEvent.Batch.KeyValues) == 0 {
			streamEvent.Batch = nil
		}
		return event
	}
	return nil
}

// Subscribe implements the Subscription interface.
func (p *partitionedStreamSubscription) Subscribe(ctx context.Context) (err error) {
	defer close(p.eventsChan)
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p.closed = true
		err = errors.CombineErrors(err, conn.Close())
	}()

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
			p.err = err
			return ctx.Err()
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

// Close closes the Subscription.
func (p *partitionedStreamSubscription) Close() {
	if !p.closed {
		p.closeChan <- struct{}{}
	}
}
