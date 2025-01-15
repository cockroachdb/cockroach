// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// Note on data APIs and datatypes.  As much as possible, the data that makes
// sense to the source cluster (e.g. checkpoint records, or subscription token, etc)
// is treated as an opaque object (e.g. []bytes) by this API.  This opacity is done
// on purpose as it abstracts the operations on the source cluster behind this API.

// CheckpointToken is emitted by a stream producer to encode information about
// what that producer has emitted, including what spans or timestamps it might
// have resolved. It is typically treated as opaque by a consumer but may be
// decoded and used by a KVSpanFrontier.
// type CheckpointToken []byte

// SubscriptionToken is an opaque identifier returned by Plan which can be used
// to subscribe to a given partition.
type SubscriptionToken []byte

// CheckpointToken is an opaque identifier which can be used to represent checkpoint
// information to start a stream processor.
type CheckpointToken []byte

// Client provides methods to stream updates from an application tenant.
// The client persists state on the system tenant, allowing a new client
// to resume from a checkpoint if a connection is lost.
type Client interface {
	// CreateForTenant initializes a stream with the source, potentially reserving any
	// required resources, such as protected timestamps, and returns an ID which
	// can be used to interact with this stream in the future.
	CreateForTenant(ctx context.Context, tenant roachpb.TenantName, req streampb.ReplicationProducerRequest) (streampb.ReplicationProducerSpec, error)

	// Destroy informs the source of the stream that it may terminate production
	// and release resources such as protected timestamps.
	// Destroy(ID StreamID) error

	// Heartbeat informs the src cluster that the consumer is live and
	// that source cluster protected timestamp _may_ be advanced up to the passed ts
	// (which may be zero if no progress has been made e.g. during backfill).
	// TODO(dt): ts -> checkpointToken.
	Heartbeat(
		ctx context.Context,
		streamID streampb.StreamID,
		consumed hlc.Timestamp,
	) (streampb.StreamReplicationStatus, error)

	// PlanPhysicalReplication returns a Topology for this stream.
	// TODO(dt): separate target argument from address argument.
	PlanPhysicalReplication(ctx context.Context, streamID streampb.StreamID) (Topology, error)

	// Subscribe opens and returns a subscription for the specified partition from
	// the specified remote stream. This is used by each consumer processor to
	// open its subscription to its partition of a larger stream.
	// TODO(dt): ts -> checkpointToken.
	Subscribe(
		ctx context.Context,
		streamID streampb.StreamID,
		consumerNode, consumerProc int32,
		spec SubscriptionToken,
		initialScanTime hlc.Timestamp,
		previousReplicatedTimes span.Frontier,
		opts ...SubscribeOption,
	) (Subscription, error)

	// Complete completes a replication stream consumption.
	Complete(ctx context.Context, streamID streampb.StreamID, successfulIngestion bool) error

	// PriorReplicationDetails returns a given tenant's "historyID" as well as the
	// historyID, if any, from which that tenant was previously replicated and the
	// timestamp as of which that replication ended.
	//
	// A HistoryID is a globally unique identifier a tenant span on some cluster,
	// that can be uniquely used to identify it and its MVCC history. It is
	// composed of a cluster ID on which a tenant's span resides and the tenant's
	// ID on that cluster, which uniquely identifies that span -- and its mvcc
	// history -- across all Cockroach clusters.
	PriorReplicationDetails(
		ctx context.Context, tenant roachpb.TenantName,
	) (id string, replicatedFrom string, activated hlc.Timestamp, _ error)

	PlanLogicalReplication(ctx context.Context, req streampb.LogicalReplicationPlanRequest) (LogicalReplicationPlan, error)
	CreateForTables(ctx context.Context, req *streampb.ReplicationProducerRequest) (*streampb.ReplicationProducerSpec, error)
	ExecStatement(ctx context.Context, cmd string, opname string, args ...interface{}) error

	// Close releases all the resources used by this client.
	Close(ctx context.Context) error
}

type subscribeConfig struct {
	// withFiltering controls whether the producer-side rangefeeds
	// should be started with the WithFiltering option which
	// elides rangefeed events.
	withFiltering bool

	// withDiff controls wiether the producer-side rangefeeds
	// should be started with the WithDiff option
	//
	// NB: Callers should note that initial scan results will not
	// contain a diff.
	withDiff bool

	// batchByteSize requests the producer emit batches up to the specified size.
	batchByteSize int64
}

type SubscribeOption func(*subscribeConfig)

// WithFiltering controls whether the producer side rangefeed is
// started with the WithFiltering option, eliding rows where
// OmitInRangefeed was set at write-time.
func WithFiltering(filteringEnabled bool) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.withFiltering = filteringEnabled
	}
}

// WithDiff controls whether the producer-side rangefeeds
// should be started with the WithDiff option
//
// NB: Callers should note that initial scan results will not
// contain a diff.
func WithDiff(enableDiff bool) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.withDiff = enableDiff
	}
}

// WithBatchSize requests the producer emit batches up to the specified size.
func WithBatchSize(bytes int64) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.batchByteSize = bytes
	}
}

// Topology is a configuration of stream partitions. These are particular to a
// stream. It specifies the number and connection uris of partitions of the
// stream.
//
// The topology also contains the tenant ID of the tenant whose keys are being
// streamed over the partitions.
type Topology struct {
	Partitions     []PartitionInfo
	SourceTenantID roachpb.TenantID
}

// PartitionConnUris returns the list of cluster uris in a topology
func (t Topology) PartitionConnUris() []ClusterUri {
	var uris []ClusterUri
	for _, partition := range t.Partitions {
		uris = append(uris, partition.ConnUri)
	}
	return uris
}

func (t Topology) SerializedClusterUris() []string {
	var uris []string
	for _, partition := range t.Partitions {
		uris = append(uris, partition.ConnUri.Serialize())
	}
	return uris
}

// PartitionInfo describes a partition of a replication stream, i.e. a set of key
// spans in a source cluster in which changes will be emitted.
type PartitionInfo struct {
	// ID is the stringified source instance ID.
	ID string
	SubscriptionToken
	SrcInstanceID int
	ConnUri       ClusterUri
	SrcLocality   roachpb.Locality
	Spans         []roachpb.Span
}

// Subscription represents subscription to a replication stream partition.
// Typical usage on the call site looks like:
//
//	ctxWithCancel, cancelFn := context.WithCancel(ctx)
//	g := ctxgroup.WithContext(ctxWithCancel)
//	sub := client.Subscribe()
//	g.GoCtx(sub.Subscribe)
//	g.GoCtx(processEventsAndErrors(sub.Events(), sub.Err()))
//	g.Wait()
type Subscription interface {
	// Subscribe starts receiving subscription events. Terminates when context
	// is cancelled. It will release all resources when the function returns.
	Subscribe(ctx context.Context) error

	// Events is a channel receiving streaming events.
	// This channel is closed when no additional values will be sent to this channel.
	Events() <-chan crosscluster.Event

	// Err is set once when Events channel closed -- must not be called before
	// the channel closes.
	Err() error
}

// NewStreamClient creates a new stream client based on the stream address.
func NewStreamClient(
	ctx context.Context, uri ClusterUri, db descs.DB, opts ...Option,
) (Client, error) {
	switch scheme := uri.URL().Scheme; scheme {
	case "postgres", "postgresql":
		// The canonical PostgreSQL URL scheme is "postgresql", however our
		// own client commands also accept "postgres".
		return NewPartitionedStreamClient(ctx, uri, opts...)
	case RandomGenScheme:
		// TODO(jeffswenson): inject a dialer in tests so we can get rid of support
		// for the "random" prefix in the ClusterConn.
		return RandomGenClientBuilder(uri, db)
	default:
		return nil, errors.Newf("stream replication from scheme %q is unsupported", scheme)
	}
}

// GetFirstActiveClient iterates through each provided stream uri
// and returns the first client it's able to successfully dial.
func GetFirstActiveClient(
	ctx context.Context, connectionUris []ClusterUri, db descs.DB, opts ...Option,
) (Client, error) {
	newClient := func(ctx context.Context, uri ClusterUri) (Client, error) {
		return NewStreamClient(ctx, uri, db, opts...)
	}
	return getFirstClient(ctx, connectionUris, newClient)
}

type clientFactory[T any] func(ctx context.Context, uri ClusterUri) (T, error)

func getFirstClient[T any](
	ctx context.Context, connectionUri []ClusterUri, getNewClient clientFactory[T],
) (T, error) {
	var zero T
	if len(connectionUri) == 0 {
		return zero, errors.Newf("failed to connect, no connection uris")
	}

	var combinedError error = nil
	for _, uri := range connectionUri {
		clientCandidate, err := getNewClient(ctx, uri)
		if err == nil {
			return clientCandidate, nil
		}
		// Note the failure and attempt the next uri
		log.Warningf(ctx, "failed to connect to uri %s: %s", uri.Redacted(), err.Error())
		combinedError = errors.CombineErrors(combinedError, err)
	}
	return zero, errors.Wrap(combinedError, "failed to connect to any connection uri")
}

type options struct {
	streamID   streampb.StreamID
	compressed bool
	logical    bool
}

func (o *options) appName() string {
	const appNameBase = "repstream"
	if o.streamID != 0 {
		return fmt.Sprintf("%s job id=%d", appNameBase, o.streamID)
	} else {
		return appNameBase
	}
}

// An Option configures some part of a Client.
type Option func(*options)

// WithStreamID sets the StreamID for the client. This only impacts
// metadata such as the application_name on the SQL connection.
func WithStreamID(id streampb.StreamID) Option {
	return func(o *options) {
		o.streamID = id
	}
}

// WithCompression controls requesting a compressed stream from the producer.
func WithCompression(enabled bool) Option {
	return func(o *options) {
		o.compressed = enabled
	}
}

func WithLogical() Option {
	return func(o *options) {
		o.logical = true
	}
}

func processOptions(opts []Option) *options {
	ret := &options{}
	for _, o := range opts {
		o(ret)
	}
	return ret
}

/*
TODO(cdc): Proposed new API from yv/dt chat. #70927.

// KVStream is a helper that wraps a client and a created stream.
type KVStream interface {
  // Returns kv stream id.
  ID() StreamID

  // Close terminates kv stream, including informing the src cluster that it may
  // release resources/protected timestamps.
  Close() error

  // Heartbeat informs the src cluster that the consumer is live and
  // that source cluster protected timestamp _may_ be advanced up to the passed ts
  // (which may be zero if no progress has been made e.g. during backfill).
  Heartbeat(consumed hlc.Timestamp) (time.Time, error)

  // Plan returns information required to initate subscription(s) to this stream
	// potentially partitioned as requested by the options.
  Plan(checkpointToken CheckpointToken, opts ParitioningOption) ([]ParitionInfo, error)
}

// Subscription represents subscription to a single stream.
type Subscription interface {
  // Events is a channel receiving streaming events.
  // This channel is closed when no additional values will be sent to this channel.
  Events() <-chan StreamSubscriptionEvent

  // Err is set onces when Events channel closed -- must not be called before
  // the channel closes.
  Err() error

  // Close must be called to release resources used by this subscription.
  Close() error
}

type StreamSubscriptionEvent interface {
	isStreamEvent()
}

type StreamKVs interface {
	AddTo(BufferingAdder)
	TS() hlc.Timestamp
}

// KVStreamFrontier is used to represent a frontier of span/time processed in a
// KVStream, and is used only in the coorinator. It wraps a span frontier and
// provides threadsafety.
type KVStreamFrontier interface {
	Advance(add CheckpointToken) (bool, error)
	MinTS() hlc.Timestamp
	GetCheckpointToken() CheckpointToken
}

// IsPermanentError returns true if the error is a permanent error that
// should cause stream to shutdown.
// TODO: This needs to be clarified; just putting this as a placeholder
// because we will need to deal w/ errors.
func IsPermanentError(err error) bool
*/
