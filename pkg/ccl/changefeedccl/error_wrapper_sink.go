// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// errorWrapperSink delegates to another sink and marks all returned errors as
// retryable. During changefeed setup, we use the sink once without this to
// verify configuration, but in the steady state, sink errors are assumed to be
// transient unless otherwise specified.
type errorWrapperSink struct {
	wrapped Sink
}

// fatalSinkError is an error wrapper that allows sinks to indicate that the
// error they're returning is not likely to be transient: it should be fixed
// by manual intervention rather than automatically retrying the changefeed.
// Examples of a fatal sink error are an expired authorization token or a
// "message too large" response to a single-event message.
// Note that unlike similar tags this does not modify the error, as the only
// thing that needs to know about it is the maybeMarkRetryableError call
// in errorWrapperSink.
type fatalSinkError struct {
	error
}

func maybeMarkRetryableError(e error) error {
	if e == nil {
		return nil
	}
	if errors.HasType(e, fatalSinkError{}) {
		return e
	}
	return changefeedbase.MarkRetryableError(e)
}

// EmitRow implements Sink interface.
func (s errorWrapperSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	return maybeMarkRetryableError(s.wrapped.EmitRow(ctx, topic, key, value, updated, mvcc, alloc))
}

// EmitResolvedTimestamp implements Sink interface.
func (s errorWrapperSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	return maybeMarkRetryableError(s.wrapped.EmitResolvedTimestamp(ctx, encoder, resolved))
}

// Flush implements Sink interface.
func (s errorWrapperSink) Flush(ctx context.Context) error {
	return maybeMarkRetryableError(s.wrapped.Flush(ctx))
}

// Close implements Sink interface.
func (s errorWrapperSink) Close() error {
	return maybeMarkRetryableError(s.wrapped.Close())
}

// Dial implements Sink interface.
func (s errorWrapperSink) Dial() error {
	return s.wrapped.Dial()
}
