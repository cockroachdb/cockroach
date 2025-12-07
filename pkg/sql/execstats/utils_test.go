// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstats

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// AddComponentStats modifies TraceAnalyzer internal state to add stats for the
// processor/stream/flow specified in stats.ComponentID and the given node ID.
func (a *TraceAnalyzer) AddComponentStats(stats *execinfrapb.ComponentStats) {
	a.FlowsMetadata.AddComponentStats(stats)
}

// AddComponentStats modifies FlowsMetadata to add stats for the
// processor/stream/flow specified in stats.ComponentID and the given node ID.
func (m *FlowsMetadata) AddComponentStats(stats *execinfrapb.ComponentStats) {
	switch stats.Component.Type {
	case execinfrapb.ComponentID_PROCESSOR:
		if m.processorStats == nil {
			m.processorStats = make(map[execinfrapb.ProcessorID]*execinfrapb.ComponentStats)
		}
		m.processorStats[execinfrapb.ProcessorID(stats.Component.ID)] = stats
	case execinfrapb.ComponentID_STREAM:
		streamStat := &streamStats{
			originSQLInstanceID: stats.Component.SQLInstanceID,
			stats:               stats,
		}
		if m.streamStats == nil {
			m.streamStats = make(map[execinfrapb.StreamID]*streamStats)
		}
		m.streamStats[execinfrapb.StreamID(stats.Component.ID)] = streamStat
	default:
		flowStat := &flowStats{}
		flowStat.stats = append(flowStat.stats, stats)
		if m.flowStats == nil {
			m.flowStats = make(map[base.SQLInstanceID]*flowStats)
		}
		m.flowStats[stats.Component.SQLInstanceID] = flowStat
	}
}

func TestGetKVAndACStats(t *testing.T) {
	tr := tracing.NewTracer()
	sp := tr.StartSpan("test", tracing.WithRecording(tracingpb.RecordingStructured))
	defer sp.Finish()

	// Record multiple contention events.
	ce1 := &kvpb.ContentionEvent{
		Key:      []byte("key1"),
		TxnMeta:  enginepb.TxnMeta{ID: uuid.MakeV4()},
		Duration: 100 * time.Millisecond,
	}
	ce2 := &kvpb.ContentionEvent{
		Key:      []byte("key2"),
		TxnMeta:  enginepb.TxnMeta{ID: uuid.MakeV4()},
		Duration: 200 * time.Millisecond,
		IsLatch:  true,
	}
	sp.RecordStructured(ce1)
	sp.RecordStructured(ce2)

	// Record admission control stats.
	acStats := &admissionpb.AdmissionWorkQueueStats{
		WaitDurationNanos: 50 * time.Millisecond,
	}
	sp.RecordStructured(acStats)

	// Record quorum replication flow admission event.
	quorumEvent := &kvpb.QuorumReplicationFlowAdmissionEvent{
		WaitDurationNanos: 75 * time.Millisecond,
	}
	sp.RecordStructured(quorumEvent)

	trace := sp.GetRecording(tracingpb.RecordingStructured)
	contentionEvents, acWaitTime := getKVAndACStats(trace)

	// Verify contention events are collected correctly and independently.
	require.Len(t, contentionEvents, 2)
	require.Equal(t, []byte("key1"), []byte(contentionEvents[0].Key))
	require.Equal(t, 100*time.Millisecond, contentionEvents[0].Duration)
	require.False(t, contentionEvents[0].IsLatch)
	require.Equal(t, []byte("key2"), []byte(contentionEvents[1].Key))
	require.Equal(t, 200*time.Millisecond, contentionEvents[1].Duration)
	require.True(t, contentionEvents[1].IsLatch)

	// Verify admission wait time is accumulated from both sources.
	require.Equal(t, 125*time.Millisecond, acWaitTime)
}
