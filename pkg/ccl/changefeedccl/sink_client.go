// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

// SinkClient is the interface to an external sink, where batches of messages
// can be encoded into a formatted payload that can be emitted to the sink, and
// these payloads can be emitted.  Emitting is a separate step to Encoding to
// allow for the same encoded payload to retry the Emitting.
type SinkClient interface {
	EncodeBatch(topic string, batch []messagePayload) (SinkPayload, error)
	EncodeResolvedMessage(resolvedMessagePayload) (SinkPayload, error)
	EmitPayload(SinkPayload) error
	Close() error
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its EmitRow method.
type SinkPayload interface{}

// messagePayload represents a KV event to be emitted.
type messagePayload struct {
	key   []byte
	val   []byte
	topic string
}

// resolvedMessagePayload represents a Resolved event to be emitted.
type resolvedMessagePayload struct {
	body  []byte
	topic string
}
