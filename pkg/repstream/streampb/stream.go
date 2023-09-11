// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streampb

import (
	fmt "fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/otel/attribute"
)

var _ tracing.TracingAggregatorEvent = (*ReplicationStreamProducerStats)(nil)
var _ tracing.TracingAggregatorEvent = (*StreamEvent_StreamStatistics)(nil)

// Identity implements the TracingAggregatorEvent interface.
func (s *StreamEvent_StreamStatistics) Identity() tracing.TracingAggregatorEvent {
	return &StreamEvent_StreamStatistics{
		Stats: make(map[int32]*ReplicationStreamProducerStats),
	}
}

// Combine implements the TracingAggregatorEvent interface.
func (s *StreamEvent_StreamStatistics) Combine(other tracing.TracingAggregatorEvent) {
	for sourceID, producerStats := range other.(*StreamEvent_StreamStatistics).Stats {
		if ps, ok := s.Stats[sourceID]; ok {
			ps.Combine(producerStats)
		} else {
			s.Stats[sourceID] = producerStats
		}
	}
}

// ProtoName implements the TracingAggregatorEvent interface.
func (s *StreamEvent_StreamStatistics) ProtoName() string {
	return proto.MessageName(s)
}

func (s *StreamEvent_StreamStatistics) ToText() []byte {
	return []byte(s.String())
}

func (s *StreamEvent_StreamStatistics) String() string {
	var buf strings.Builder
	for sourceID, ps := range s.Stats {
		fmt.Fprintf(&buf, "%d: %s", sourceID, ps.String())
	}
	return buf.String()
}

// Render implements the TracingAggregatorEvent interface.
func (s *StreamEvent_StreamStatistics) Render() []attribute.KeyValue {
	return nil
}

// Identity implements the TracingAggregatorEvent interface.
func (s *ReplicationStreamProducerStats) Identity() tracing.TracingAggregatorEvent {
	return &ReplicationStreamProducerStats{}
}

// Combine implements the TracingAggregatorEvent interface.
func (s *ReplicationStreamProducerStats) Combine(_other tracing.TracingAggregatorEvent) {
}

// ProtoName implements the TracingAggregatorEvent interface.
func (s *ReplicationStreamProducerStats) ProtoName() string {
	return proto.MessageName(s)
}

func (s *ReplicationStreamProducerStats) ToText() []byte {
	return []byte(s.String())
}

func (s *ReplicationStreamProducerStats) String() string {
	return "HELO"
}

// Render implements the TracingAggregatorEvent interface.
func (s *ReplicationStreamProducerStats) Render() []attribute.KeyValue {
	return nil
}
