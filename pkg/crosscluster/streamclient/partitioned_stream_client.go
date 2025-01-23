// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

type partitionedStreamClient struct {
	clusterUri ClusterUri
	pgxConfig  *pgx.ConnConfig
	compressed bool
	logical    bool

	mu struct {
		syncutil.Mutex

		closed              bool
		activeSubscriptions map[*partitionedStreamSubscription]struct{}
		srcConn             *pgx.Conn // pgx connection to the source cluster
	}
}

func NewPartitionedStreamClient(
	ctx context.Context, remote ClusterUri, opts ...Option,
) (*partitionedStreamClient, error) {
	options := processOptions(opts)
	conn, config, err := newPGConnForClient(ctx, remote.URL(), options)
	if err != nil {
		return nil, err
	}
	client := partitionedStreamClient{
		clusterUri: remote,
		pgxConfig:  config,
		compressed: options.compressed,
		logical:    options.logical,
	}
	client.mu.activeSubscriptions = make(map[*partitionedStreamSubscription]struct{})
	client.mu.srcConn = conn

	if err := client.dial(ctx); err != nil {
		return nil, err
	}

	return &client, nil
}

var _ Client = &partitionedStreamClient{}

// CreateForTenant implements Client interface.
func (p *partitionedStreamClient) CreateForTenant(
	ctx context.Context, tenantName roachpb.TenantName, req streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	ctx, sp := tracing.ChildSpan(ctx, "Client.CreateForTenant")
	defer sp.Finish()
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.logical {
		return streampb.ReplicationProducerSpec{}, errors.New("cannot create a tenant scoped stream with logical replication flag")
	}

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
func (p *partitionedStreamClient) dial(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.mu.srcConn.Ping(ctx)
	return errors.Wrap(err, "failed to dial client")
}

// Heartbeat implements Client interface.
func (p *partitionedStreamClient) Heartbeat(
	ctx context.Context, streamID streampb.StreamID, consumed hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	ctx, sp := tracing.ChildSpan(ctx, "Client.Heartbeat")
	defer sp.Finish()

	p.mu.Lock()
	defer p.mu.Unlock()

	// The heartbeat function is called in a loop with the same connection. If
	// that connection closes, attempt to re-establish it to prevent the hearbeat
	// loop from failing repeatedly.
	if p.mu.srcConn.IsClosed() {
		conn, err := pgx.ConnectConfig(ctx, p.pgxConfig)
		if err != nil {
			return streampb.StreamReplicationStatus{}, errors.Wrapf(err, "error reconnecting to replication stream %d", streamID)
		}
		p.mu.srcConn = conn
		if err := p.mu.srcConn.Ping(ctx); err != nil {
			return streampb.StreamReplicationStatus{}, errors.Wrapf(err, "error pinging on reconnecting replication stream %d", streamID)
		}
	}

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

// PlanPhysicalReplication implements Client interface.
func (p *partitionedStreamClient) PlanPhysicalReplication(
	ctx context.Context, streamID streampb.StreamID,
) (Topology, error) {
	var spec streampb.ReplicationStreamSpec
	{
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.logical {
			return Topology{}, errors.New("cannot plan physical replication with logical replication flag")
		}

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
		var connUri ClusterUri
		if p.clusterUri.RoutingMode() == RoutingModeGateway {
			connUri = p.clusterUri
		} else {
			var err error
			connUri, err = MakeClusterUriForNode(p.clusterUri, sp.SQLAddress)
			if err != nil {
				return Topology{}, err
			}
		}

		rawSpec, err := protoutil.Marshal(sp.SourcePartition)
		if err != nil {
			return Topology{}, err
		}
		topology.Partitions = append(topology.Partitions, PartitionInfo{
			ID:                sp.NodeID.String(),
			SubscriptionToken: SubscriptionToken(rawSpec),
			SrcInstanceID:     int(sp.NodeID),
			ConnUri:           connUri,
			SrcLocality:       sp.Locality,
			Spans:             sp.SourcePartition.Spans,
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
	consumerNode int32,
	consumerProc int32,
	spec SubscriptionToken,
	initialScanTime hlc.Timestamp,
	previousReplicatedTimes span.Frontier,
	opts ...SubscribeOption,
) (Subscription, error) {
	_, sp := tracing.ChildSpan(ctx, "Client.Subscribe")
	defer sp.Finish()

	cfg := &subscribeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	sourcePartition := streampb.SourcePartition{}
	if err := protoutil.Unmarshal(spec, &sourcePartition); err != nil {
		return nil, err
	}

	sps := streampb.StreamPartitionSpec{}
	sps.InitialScanTimestamp = initialScanTime
	if previousReplicatedTimes != nil {
		sps.PreviousReplicatedTimestamp = previousReplicatedTimes.Frontier()
		previousReplicatedTimes.Entries(func(s roachpb.Span, t hlc.Timestamp) (done span.OpResult) {
			sps.Progress = append(sps.Progress, jobspb.ResolvedSpan{Span: s, Timestamp: t})
			return span.ContinueMatch
		})
	}
	sps.Spans = sourcePartition.Spans
	sps.ConsumerNode = consumerNode
	sps.ConsumerProc = consumerProc
	sps.Compressed = true
	sps.WrappedEvents = true
	sps.WithDiff = cfg.withDiff
	sps.WithFiltering = cfg.withFiltering
	sps.Type = streampb.ReplicationType_PHYSICAL
	sps.Config.BatchByteSize = cfg.batchByteSize
	if p.logical {
		sps.Type = streampb.ReplicationType_LOGICAL
	}

	specBytes, err := protoutil.Marshal(&sps)
	if err != nil {
		return nil, err
	}

	res := &partitionedStreamSubscription{
		eventsChan:    make(chan crosscluster.Event),
		srcConnConfig: p.pgxConfig,
		specBytes:     specBytes,
		streamID:      streamID,
		closeChan:     make(chan struct{}),
		compressed:    sps.Compressed,
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.activeSubscriptions[res] = struct{}{}
	return res, nil
}

// Complete implements the Client interface.
func (p *partitionedStreamClient) Complete(
	ctx context.Context, streamID streampb.StreamID, successfulIngestion bool,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "Client.Complete")
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

type LogicalReplicationPlan struct {
	Topology      Topology
	SourceSpans   []roachpb.Span
	DescriptorMap map[int32]descpb.TableDescriptor
	SourceTypes   []*descpb.TypeDescriptor
}

func (p *partitionedStreamClient) PlanLogicalReplication(
	ctx context.Context, req streampb.LogicalReplicationPlanRequest,
) (LogicalReplicationPlan, error) {
	ctx, sp := tracing.ChildSpan(ctx, "Client.PlanLogicalReplication")
	defer sp.Finish()

	if !p.logical {
		return LogicalReplicationPlan{}, errors.New("cannot plan logical replication without logical replication flag")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	encodedReq, err := protoutil.Marshal(&req)
	if err != nil {
		return LogicalReplicationPlan{}, err
	}

	row := p.mu.srcConn.QueryRow(ctx,
		fmt.Sprintf("SELECT * FROM crdb_internal.plan_logical_replication($1) AS OF SYSTEM TIME %s", req.PlanAsOf.AsOfSystemTime()),
		encodedReq)

	streamSpecBytes := []byte{}
	if err := row.Scan(&streamSpecBytes); err != nil {
		return LogicalReplicationPlan{}, err
	}

	streamSpec := streampb.ReplicationStreamSpec{}
	if err := protoutil.Unmarshal(streamSpecBytes, &streamSpec); err != nil {
		return LogicalReplicationPlan{}, err
	}
	topology, err := p.createTopology(streamSpec)
	if err != nil {
		return LogicalReplicationPlan{}, err
	}

	descMap := make(map[int32]descpb.TableDescriptor)
	for _, desc := range streamSpec.TableDescriptors {
		desc := desc
		descMap[int32(desc.ID)] = desc
	}

	sourceTypes := make([]*descpb.TypeDescriptor, len(streamSpec.TypeDescriptors))
	for i, desc := range streamSpec.TypeDescriptors {
		sourceTypes[i] = &desc
	}

	return LogicalReplicationPlan{
		Topology:      topology,
		SourceSpans:   streamSpec.TableSpans,
		DescriptorMap: descMap,
		SourceTypes:   sourceTypes,
	}, nil
}

func (p *partitionedStreamClient) CreateForTables(
	ctx context.Context, req *streampb.ReplicationProducerRequest,
) (*streampb.ReplicationProducerSpec, error) {
	ctx, sp := tracing.ChildSpan(ctx, "Client.CreateForTables")
	defer sp.Finish()

	if !p.logical {
		return nil, errors.New("cannot create a table scoped stream without logical replication flag")
	}

	reqBytes, err := protoutil.Marshal(req)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.mu.srcConn.QueryRow(ctx, "SELECT crdb_internal.start_replication_stream_for_tables($1)", reqBytes)
	specBytes := []byte{}
	if err := r.Scan(&specBytes); err != nil {
		return nil, err
	}

	spec := &streampb.ReplicationProducerSpec{}
	if err := protoutil.Unmarshal(specBytes, spec); err != nil {
		return nil, err
	}
	return spec, nil
}
func (p *partitionedStreamClient) ExecStatement(
	ctx context.Context, cmd string, opname string, args ...interface{},
) error {
	ctx, sp := tracing.ChildSpan(ctx, opname)
	defer sp.Finish()

	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.mu.srcConn.Exec(ctx, cmd, args...)
	return err
}

// PriorReplicationDetails implements the Client interface.
func (p *partitionedStreamClient) PriorReplicationDetails(
	ctx context.Context, tenant roachpb.TenantName,
) (string, string, hlc.Timestamp, error) {
	ctx, sp := tracing.ChildSpan(ctx, "Client.PriorReplicationDetails")
	defer sp.Finish()

	var id string
	var fromID, activated gosql.NullString
	p.mu.Lock()
	defer p.mu.Unlock()
	row := p.mu.srcConn.QueryRow(ctx,
		`SELECT crdb_internal.cluster_id()::string||':'||id::string, source_id, activation_time FROM [SHOW VIRTUAL CLUSTER $1 WITH PRIOR REPLICATION DETAILS]`, tenant)
	if err := row.Scan(&id, &fromID, &activated); err != nil {
		return "", "", hlc.Timestamp{}, errors.Wrapf(err, "error querying prior replication details for %s", tenant)
	}

	if activated.Valid {
		d, _, err := apd.NewFromString(activated.String)
		if err != nil {
			return "", "", hlc.Timestamp{}, err
		}
		ts, err := hlc.DecimalToHLC(d)
		return id, fromID.String, ts, err
	}
	return id, "", hlc.Timestamp{}, nil
}

type partitionedStreamSubscription struct {
	err           error
	srcConnConfig *pgx.ConnConfig
	eventsChan    chan crosscluster.Event
	// Channel to send signal to close the subscription.
	closeChan chan struct{}

	compressed bool

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

	// Set statement_timeout to 0s for no statement timeout. We
	// expect crdb_internal.stream_partition to run forevever.
	_, err = srcConn.Exec(ctx, `SET statement_timeout = '0s'`)
	if err != nil {
		return err
	}

	rows, err := srcConn.Query(ctx, `SELECT * FROM crdb_internal.stream_partition($1, $2)`,
		p.streamID, p.specBytes)
	if err != nil {
		return err
	}
	defer rows.Close()

	p.err = subscribeInternal(ctx, rows, p.eventsChan, p.closeChan, p.compressed)
	return p.err
}

// Events implements the Subscription interface.
func (p *partitionedStreamSubscription) Events() <-chan crosscluster.Event {
	return p.eventsChan
}

// Err implements the Subscription interface.
func (p *partitionedStreamSubscription) Err() error {
	return p.err
}
