// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

// flushTypeURL identifies the marshaled revlogpb.Flush carried in
// BulkProcessorProgress.ProgressDetails.
const flushTypeURL = "type.googleapis.com/cockroach.revlog.revlogpb.Flush"

// metaSink is a TickSink implementation that publishes each Flush
// to a channel as a BulkProcessorProgress entry. The producer-side
// distsql processor wraps this sink around its progCh; on each
// Flush, the sink marshals the message and emits it. The
// processor's Next() reads from the chan and surfaces each entry as
// a ProducerMetadata. The gateway's metadata callback decodes the
// Flush back out and applies it to the gateway-side TickManager.
type metaSink struct {
	out chan<- execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
}

func newMetaSink(out chan<- execinfrapb.RemoteProducerMetadata_BulkProcessorProgress) *metaSink {
	return &metaSink{out: out}
}

// Flush implements TickSink. Returns ctx.Err() if the chan can't
// accept before ctx is done.
func (s *metaSink) Flush(ctx context.Context, msg *revlogpb.Flush) error {
	body, err := protoutil.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "marshaling Flush")
	}
	prog := execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
		ProgressDetails: gogotypes.Any{TypeUrl: flushTypeURL, Value: body},
	}
	select {
	case s.out <- prog:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DecodeFlush is the inverse used by the gateway-side metadata
// callback: given the ProgressDetails bytes from a received
// BulkProcessorProgress, returns the parsed Flush.
func DecodeFlush(any gogotypes.Any) (*revlogpb.Flush, error) {
	var f revlogpb.Flush
	if err := protoutil.Unmarshal(any.Value, &f); err != nil {
		return nil, errors.Wrap(err, "decoding Flush")
	}
	return &f, nil
}
