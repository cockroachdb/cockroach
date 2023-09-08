// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streamingest

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/otel/attribute"
)

func (h *HistogramData) String() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("min: %.6f\n", float64(h.Min)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("max: %.6f\n", float64(h.Max)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p5: %.6f\n", float64(h.P5)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p50: %.6f\n", float64(h.P50)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p90: %.6f\n", float64(h.P90)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p99: %.6f\n", float64(h.P99)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("p99_9: %.6f\n", float64(h.P99_9)/float64(time.Second)))
	b.WriteString(fmt.Sprintf("mean: %.6f\n", h.Mean/float32(time.Second)))
	b.WriteString(fmt.Sprintf("count: %d\n", h.Count))

	return b.String()
}

var _ bulk.TracingAggregatorEvent = &StreamIngestionStats{}
var _ jobs.ProtobinExecutionDetailFile = &StreamIngestionStats{}

func (m *StreamIngestionStats) Render() []attribute.KeyValue {
	return nil
}

func (m *StreamIngestionStats) Identity() bulk.TracingAggregatorEvent {
	return &StreamIngestionStats{}
}

func (m *StreamIngestionStats) Combine(other bulk.TracingAggregatorEvent) {
	otherStats, ok := other.(*StreamIngestionStats)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type StreamSubscriptionStats: %T", other))
	}
	m.TimeInHandleEvent = otherStats.TimeInHandleEvent
	m.TimeBetweenHandleEvents = otherStats.TimeBetweenHandleEvents
}

func (m *StreamIngestionStats) ProtoName() string {
	return proto.MessageName(m)
}

func (m *StreamIngestionStats) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("time_in_handle: \n%s\n", m.TimeInHandleEvent.String()))
	b.WriteString(fmt.Sprintf("time_between_handle: \n%s\n", m.TimeBetweenHandleEvents.String()))
	return b.String()
}

func (m *StreamIngestionStats) ToText() []byte {
	return []byte(m.String())
}
