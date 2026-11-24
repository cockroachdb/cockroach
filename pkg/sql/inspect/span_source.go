// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// spanSource defines a source of spans for the INSPECT processor to consume.
// It allows testing and production implementations to supply spans dynamically.
type spanSource interface {
	// NextSpan returns the next span to process.
	// ok is false when there are no more spans.
	// error is returned on fatal errors.
	NextSpan(ctx context.Context) (roachpb.Span, bool, error)
}

// sliceSpanSource is a spanSource implementation backed by a fixed slice of spans.
// It emits spans sequentially in the order they appear in the slice.
type sliceSpanSource struct {
	spans []roachpb.Span
	index int
}

var _ spanSource = (*sliceSpanSource)(nil)

func newSliceSpanSource(spans []roachpb.Span) *sliceSpanSource {
	return &sliceSpanSource{spans: spans}
}

// NextSpan implements the spanSource interface.
func (s *sliceSpanSource) NextSpan(ctx context.Context) (roachpb.Span, bool, error) {
	if s.index >= len(s.spans) {
		return roachpb.Span{}, false, nil
	}
	span := s.spans[s.index]
	s.index++
	return span, true, nil
}
