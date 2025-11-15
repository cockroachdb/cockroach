// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

const sendQueueLoggingInterval = 30 * time.Second

// SendQueueLogger logs send queue sizes for streams.
//
// Usage:
//
//	sql := NewSendQueueLogger(numStreamsToLog)
//	// Periodically do:
//	if sql.TryStartLog() {
//	    for each range:
//	        sql.ObserveRangeStats(rangeStats)
//	    sql.FinishLog(ctx)
//	}
//
// It is not thread-safe.
type SendQueueLogger struct {
	numStreamsToLog int
	limiter         log.EveryN
	scratchMap      map[kvflowcontrol.Stream]int64
	scratchSlice    []streamAndBytes
}

type streamAndBytes struct {
	stream kvflowcontrol.Stream
	bytes  int64
}

// NewSendQueueLogger creates a new SendQueueLogger that will log up to
// numStreamsToLog streams with the highest send queue bytes when logging is
// triggered.
func NewSendQueueLogger(numStreamsToLog int) *SendQueueLogger {
	return &SendQueueLogger{
		numStreamsToLog: numStreamsToLog,
		limiter:         log.Every(sendQueueLoggingInterval),
		scratchMap:      make(map[kvflowcontrol.Stream]int64),
	}
}

func (s *SendQueueLogger) TryStartLog() bool {
	return s.limiter.ShouldLog()
}

func (s *SendQueueLogger) ObserveRangeStats(stats *RangeSendStreamStats) {
	for _, ss := range stats.internal {
		if ss.SendQueueBytes > 0 {
			s.scratchMap[ss.Stream] = s.scratchMap[ss.Stream] + ss.SendQueueBytes
		}
	}
}

func (s *SendQueueLogger) FinishLog(ctx context.Context) {
	if len(s.scratchMap) == 0 {
		return
	}
	defer func() {
		clear(s.scratchMap)
		s.scratchSlice = s.scratchSlice[:0]
	}()
	for stream, bytes := range s.scratchMap {
		s.scratchSlice = append(s.scratchSlice, streamAndBytes{
			stream: stream,
			bytes:  bytes,
		})
	}
	// Sort by descending send queue bytes.
	slices.SortFunc(s.scratchSlice, func(a, b streamAndBytes) int {
		return -cmp.Compare(a.bytes, b.bytes)
	})
	var b strings.Builder
	n := min(s.numStreamsToLog, len(s.scratchSlice))
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s: %s", s.scratchSlice[i].stream,
			humanize.IBytes(uint64(s.scratchSlice[i].bytes)))
	}
	remainingBytes := int64(0)
	for i := n; i < len(s.scratchSlice); i++ {
		remainingBytes += s.scratchSlice[i].bytes
	}
	if remainingBytes > 0 {
		fmt.Fprintf(&b, ", + %s more bytes across %d streams",
			humanize.IBytes(uint64(remainingBytes)), len(s.scratchSlice)-n)
	}
	log.KvExec.Infof(ctx, "send queues: %s", redact.SafeString(b.String()))
}
