// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// Dialer is a wrapper for different kinds of clients which stream updates from
// a tenant.
type Dialer interface {
	// Dial checks if the source is able to be connected to for queries
	Dial(ctx context.Context) error

	// Close releases all the resources used by this client.
	Close(ctx context.Context) error
}

// Client provides methods to stream updates from an application tenant.
// The client persists state on the system tenant, allowing a new client
// to resume from a checkpoint if a connection is lost.
type Client interface {
	Dialer

	// Create initializes a stream with the source, potentially reserving any
	// required resources, such as protected timestamps, and returns an ID which
	// can be used to interact with this stream in the future.
	Create(ctx context.Context, tenant roachpb.TenantName, req streampb.ReplicationProducerRequest) (streampb.ReplicationProducerSpec, error)

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

	// Plan returns a Topology for this stream.
	// TODO(dt): separate target argument from address argument.
	Plan(ctx context.Context, streamID streampb.StreamID) (Topology, error)

	// Subscribe opens and returns a subscription for the specified partition from
	// the specified remote address. This is used by each consumer processor to
	// open its subscription to its partition of a larger stream.
	// TODO(dt): ts -> checkpointToken.
	Subscribe(ctx context.Context, streamID streampb.StreamID, spec SubscriptionToken,
		initialScanTime hlc.Timestamp, previousReplicatedTime hlc.Timestamp) (Subscription, error)

	// Complete completes a replication stream consumption.
	Complete(ctx context.Context, streamID streampb.StreamID, successfulIngestion bool) error
}

// Topology is a configuration of stream partitions. These are particular to a
// stream. It specifies the number and addresses of partitions of the stream.
//
// The topology also contains the tenant ID of the tenant whose keys are being
// streamed over the partitions.
type Topology struct {
	Partitions     []PartitionInfo
	SourceTenantID roachpb.TenantID
}

// StreamAddresses returns the list of source addresses in a topology
func (t Topology) StreamAddresses() []string {
	var addresses []string
	for _, partition := range t.Partitions {
		addresses = append(addresses, string(partition.SrcAddr))
	}
	return addresses
}

// PartitionInfo describes a partition of a replication stream, i.e. a set of key
// spans in a source cluster in which changes will be emitted.
type PartitionInfo struct {
	// ID is the stringified source instance ID.
	ID string
	SubscriptionToken
	SrcInstanceID int
	SrcAddr       streamingccl.PartitionAddress
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
	Events() <-chan streamingccl.Event

	// Err is set once when Events channel closed -- must not be called before
	// the channel closes.
	Err() error
}

// NewStreamClient creates a new stream client based on the stream address.
func NewStreamClient(
	ctx context.Context, streamAddress streamingccl.StreamAddress, db isql.DB, opts ...Option,
) (Client, error) {
	var streamClient Client
	streamURL, err := streamAddress.URL()
	if err != nil {
		return streamClient, err
	}

	switch streamURL.Scheme {
	case "postgres", "postgresql":
		// The canonical PostgreSQL URL scheme is "postgresql", however our
		// own client commands also accept "postgres".
		return NewPartitionedStreamClient(ctx, streamURL, opts...)
	case "external":
		if db == nil {
			return nil, errors.AssertionFailedf("nil db handle can't be used to dereference external URI")
		}
		addr, err := lookupExternalConnection(ctx, streamURL.Host, db)
		if err != nil {
			return nil, err
		}
		return NewStreamClient(ctx, addr, db, opts...)
	case RandomGenScheme:
		streamClient, err = newRandomStreamClient(streamURL)
		if err != nil {
			return streamClient, err
		}
	default:
		return nil, errors.Newf("stream replication from scheme %q is unsupported", streamURL.Scheme)
	}

	return streamClient, nil
}

func lookupExternalConnection(
	ctx context.Context, name string, localDB isql.DB,
) (streamingccl.StreamAddress, error) {
	// Retrieve the external connection object from the system table.
	var ec externalconn.ExternalConnection
	if err := localDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		ec, err = externalconn.LoadExternalConnection(ctx, name, txn)
		return err
	}); err != nil {
		return "", errors.Wrap(err, "failed to load external connection object")
	}
	return streamingccl.StreamAddress(ec.ConnectionProto().UnredactedURI()), nil
}

// GetFirstActiveClient iterates through each provided stream address
// and returns the first client it's able to successfully dial.
func GetFirstActiveClient(
	ctx context.Context, streamAddresses []string, db isql.DB, opts ...Option,
) (Client, error) {

	newClient := func(ctx context.Context, address streamingccl.StreamAddress) (Dialer, error) {
		return NewStreamClient(ctx, address, db, opts...)
	}
	dialer, err := getFirstDialer(ctx, streamAddresses, newClient)
	if err != nil {
		return nil, err
	}
	return dialer.(Client), err
}

type dialerFactory func(ctx context.Context, address streamingccl.StreamAddress) (Dialer, error)

func getFirstDialer(
	ctx context.Context, streamAddresses []string, getNewDialer dialerFactory,
) (Dialer, error) {
	if len(streamAddresses) == 0 {
		return nil, errors.Newf("failed to connect, no addresses")
	}
	var combinedError error = nil
	for _, address := range streamAddresses {
		streamAddress := streamingccl.StreamAddress(address)
		clientCandidate, err := getNewDialer(ctx, streamAddress)
		if err == nil {
			err = clientCandidate.Dial(ctx)
			if err == nil {
				return clientCandidate, err
			}
		}
		// Note the failure and attempt the next address
		redactedAddress, errRedact := RedactSourceURI(streamAddress.String())
		if errRedact != nil {
			log.Warning(ctx, "failed to redact stream address")
		}
		log.Errorf(ctx, "failed to connect to address %s: %s", redactedAddress, err.Error())
		combinedError = errors.CombineErrors(combinedError, err)
	}
	return nil, errors.Wrap(combinedError, "failed to connect to any address")
}

type options struct {
	streamID streampb.StreamID
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

func processOptions(opts []Option) *options {
	ret := &options{}
	for _, o := range opts {
		o(ret)
	}
	return ret
}

func RedactSourceURI(addr string) (string, error) {
	return cloud.SanitizeExternalStorageURI(addr, RedactableURLParameters)
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
