// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Client provides a way for the stream ingestion job to consume a
// specified stream.
// TODO(57427): The stream client does not yet support the concept of
//  generations in a stream.
type Client interface {
	// GetTopology returns the Topology of a stream.
	GetTopology(address streamingccl.StreamAddress) (streamingccl.Topology, error)

	// ConsumePartition returns a channel on which we can start listening for
	// events from a given partition that occur after a startTime.
	//
	// Canceling the context will stop reading the partition and close the event
	// channel.
	ConsumePartition(ctx context.Context, address streamingccl.PartitionAddress, startTime hlc.Timestamp) (chan streamingccl.Event, chan error, error)
}

// NewStreamClient creates a new stream client based on the stream
// address.
func NewStreamClient(streamAddress streamingccl.StreamAddress) (Client, error) {
	var streamClient Client
	streamURL, err := streamAddress.URL()
	if err != nil {
		return streamClient, err
	}

	switch streamURL.Scheme {
	case "postgres", "postgresql":
		// The canonical PostgreSQL URL scheme is "postgresql", however our
		// own client commands also accept "postgres".
		return &sinklessReplicationClient{}, nil
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

/*
TODO(cdc): Proposed new API from yv/dt chat. #70927.

type StreamID int64

func CreateStream(gatewayAddr, target, evalCtx) (KVStream, error)

// Note on data APIs and datatypes.  As much as possible, the data that makes
// sense to the source cluster (e.g. checkpoint records, or subscription token, etc)
// is treated as an opaque object (e.g. []bytes) by this API.  This opacity is done
// on purpuse as it abstracts the operations on the source cluster behind this API.

// KVStream coordinations KV event production from the source cluster.
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

type CheckpointToken []byte

type ParitioningOptions struct {
	// Oversplit, if non-zero, splits the spans assigned into a given instance in
	// the source cluster into that many buckets (i.e. more processors per node).
  Oversplit int
}

// ParitionInfo describes a partition of a replication stream, i.e. a set of key
// spans in a source cluster in which changes will be emitted.
type ParitionInfo struct {
	ParitionID int

  // SubscriptionToken is passed to Subscribe to get a KV stream for this
  // partition. It is created by and meaningful only to the src cluster.
  SubscriptionToken []byte

  // Src describes an instance in src cluster which it suggests the consumer use
  // to open a subscription to this partition.
  Src struct {
    InstanceID int // src cluster's preferred node/instance for this partition.
    Addr util.UnresolvedAddr // sql host/port of preferred instance
    roachpb.Locality
  }
}

// Subscribe opens and returns a subscription for the specified partition from
// the specified remote address. This is used by each consumer processor to open
// its subscription to its partition of a larger stream.
func Subscribe(
	stream StreamID,
	addr util.UnresolvedAddr,
	spec SubscriptionToken,
	checkpoint CheckpointToken,
) (Subscription, error)

// Subscription represents subscription to a single stream.
type Subscription interface {
  // Events is a channel receiving streaming events.
  // This channel is closed when no additional values will be sent to this channel.
  Events() <-chan StreamSubscriptionEvent

  // Err is set onces when Events channel closed -- must not be called before
  // the channel closes.
  Err() error

  // Close must be called to release resources used by this subscirption.
  Close() error
}

type StreamSubscriptionEvent interface {
	isStreamEvent()
}
type StreamKVs interface {
	AddTo(BufferingAdder)
	TS() hlc.Timestamp
}

type StreamCheckpoint CheckpointToken

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
