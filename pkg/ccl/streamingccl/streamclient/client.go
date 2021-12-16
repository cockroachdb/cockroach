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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Note on data APIs and datatypes.  As much as possible, the data that makes
// sense to the source cluster (e.g. checkpoint records, or subscription token, etc)
// is treated as an opaque object (e.g. []bytes) by this API.  This opacity is done
// on purpose as it abstracts the operations on the source cluster behind this API.

// StreamID identifies a stream across both its producer and consumer. It is
// used when the consumer wishes to interact with the stream's producer.
type StreamID uint64

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

// StreamPausedError is returned when the replication stream is paused.
type StreamPausedError struct {
	streamID StreamID
}

// Error makes StreamPausedError an error.
func (e *StreamPausedError) Error() string {
	return fmt.Sprintf("Replication stream %d is paused in the source cluster", e.streamID)
}

// StreamInactiveError is returned when the replication stream is not active.
type StreamInactiveError struct {
	streamID StreamID
}

// Error makes StreamInactiveError an error.
func (e *StreamInactiveError) Error() string {
	return fmt.Sprintf("Replication stream %d is not active in the source cluster", e.streamID)
}

// StreamStatusUnknownError is returned when the replication stream is in unknown status.
type StreamStatusUnknownError struct {
	streamID StreamID
}

// Error makes StreamStatusUnknownError an error.
func (e *StreamStatusUnknownError) Error() string {
	return fmt.Sprintf("Replication stream %d is in unknown status in the source cluster", e.streamID)
}

// MakeReplicationStreamError converts unhealthy jobspb.StreamReplicationStatus to an error.
func MakeReplicationStreamError(streamID StreamID, status jobspb.StreamReplicationStatus) error {
	switch status.StreamStatus {
	case jobspb.StreamReplicationStatus_STREAM_ACTIVE:
		return nil
	case jobspb.StreamReplicationStatus_STREAM_INACTIVE:
		return &StreamInactiveError{streamID: streamID}
	case jobspb.StreamReplicationStatus_STREAM_PAUSED:
		return &StreamPausedError{streamID: streamID}
	case jobspb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY:
		return &StreamStatusUnknownError{streamID: streamID}
	default:
		return errors.Errorf("unknown replication stream status: %d", status.StreamStatus)
	}
}

// Client provides a way for the stream ingestion job to consume a
// specified stream.
// TODO(57427): The stream client does not yet support the concept of
//  generations in a stream.
type Client interface {
	// Create initializes a stream with the source, potentially reserving any
	// required resources, such as protected timestamps, and returns an ID which
	// can be used to interact with this stream in the future.
	Create(ctx context.Context, tenantID roachpb.TenantID) (StreamID, error)

	// Destroy informs the source of the stream that it may terminate production
	// and release resources such as protected timestamps.
	// Destroy(ID StreamID) error

	// Heartbeat informs the src cluster that the consumer is live and
	// that source cluster protected timestamp _may_ be advanced up to the passed ts
	// (which may be zero if no progress has been made e.g. during backfill).
	// TODO(dt): ts -> checkpointToken.
	Heartbeat(ctx context.Context, ID StreamID, consumed hlc.Timestamp) error

	// Plan returns a Topology for this stream.
	// TODO(dt): separate target argument from address argument.
	Plan(ctx context.Context, ID StreamID) (Topology, error)

	// Subscribe opens and returns a subscription for the specified partition from
	// the specified remote address. This is used by each consumer processor to
	// open its subscription to its partition of a larger stream.
	// TODO(dt): ts -> checkpointToken, return -> Subscription.
	Subscribe(
		ctx context.Context,
		stream StreamID,
		spec SubscriptionToken,
		checkpoint hlc.Timestamp,
	) (chan streamingccl.Event, chan error, error)
}

// Topology is a configuration of stream partitions. These are particular to a
// stream. It specifies the number and addresses of partitions of the stream.
type Topology []PartitionInfo

// PartitionInfo describes a partition of a replication stream, i.e. a set of key
// spans in a source cluster in which changes will be emitted.
type PartitionInfo struct {
	ID string
	SubscriptionToken
	SrcInstanceID int
	SrcAddr       streamingccl.PartitionAddress
	SrcLocality   roachpb.Locality
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
		return newPGWireReplicationClient(streamURL)
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
