// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

const sendQueueLoggingInterval = 30 * time.Second

// SendQueueLogger logs send queue sizes for streams.
//
// Usage:
//
//		sql := NewSendQueueLogger(numStreamsToLog)
//		// Periodically do:
//	 if coll, ok := sql.TryStartLog(); ok {
//		    for each range:
//		        coll.ObserveRangeStats(rangeStats)
//		    sql.FinishLog(ctx, coll)
//		}
//
// It is thread-safe.
type SendQueueLogger struct {
	numStreamsToLog int
	limiter         log.EveryN

	mu struct {
		syncutil.Mutex
		scratch SendQueueLoggerCollector
	}

	testingLog func(context.Context, *redact.StringBuilder)
}

type streamAndBytes struct {
	stream kvflowcontrol.Stream
	bytes  int64
}

type SendQueueLoggerCollector struct {
	m  map[kvflowcontrol.Stream]int64
	sl []streamAndBytes
}

// NewSendQueueLogger creates a new SendQueueLogger that will log up to
// numStreamsToLog streams with the highest send queue bytes when logging is
// triggered.
func NewSendQueueLogger(numStreamsToLog int) *SendQueueLogger {
	return &SendQueueLogger{
		numStreamsToLog: numStreamsToLog,
		limiter:         log.Every(sendQueueLoggingInterval),
	}
}

func (s *SendQueueLogger) TryStartLog() (SendQueueLoggerCollector, bool) {
	if !s.limiter.ShouldLog() {
		return SendQueueLoggerCollector{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	collector := s.mu.scratch
	s.mu.scratch = SendQueueLoggerCollector{}
	if collector.m == nil {
		collector.m = make(map[kvflowcontrol.Stream]int64)
	}
	return collector, true
}

func (s *SendQueueLoggerCollector) ObserveRangeStats(stats *RangeSendStreamStats) {
	for _, ss := range stats.internal {
		if ss.SendQueueBytes > 0 {
			s.m[ss.Stream] = s.m[ss.Stream] + ss.SendQueueBytes
		}
	}
}

// NB: this is defined here so we can access it from tests.
const sendQueueLogFormat = "send queues: %s"

func (s *SendQueueLogger) FinishLog(ctx context.Context, c SendQueueLoggerCollector) {
	if len(c.m) == 0 {
		return
	}
	defer func() {
		clear(c.m)
		c.sl = c.sl[:0]
		s.mu.Lock()
		s.mu.scratch = c
		s.mu.Unlock()
	}()
	var buf redact.StringBuilder
	c.finishLog(s.numStreamsToLog, &buf)
	if fn := s.testingLog; fn != nil {
		fn(ctx, &buf)
	} else {
		log.KvDistribution.Infof(ctx, sendQueueLogFormat, &buf)
	}
}

func (c *SendQueueLoggerCollector) finishLog(maxNumStreams int, buf *redact.StringBuilder) {
	for stream, bytes := range c.m {
		c.sl = append(c.sl, streamAndBytes{
			stream: stream,
			bytes:  bytes,
		})
	}
	// Sort by descending send queue bytes.
	slices.SortFunc(c.sl, func(a, b streamAndBytes) int {
		return -cmp.Compare(a.bytes, b.bytes)
	})
	n := min(maxNumStreams, len(c.sl))
	for i := 0; i < n; i++ {
		if i > 0 {
			buf.SafeString(", ")
		}
		buf.Printf("%s: %s", c.sl[i].stream,
			humanizeutil.IBytes(c.sl[i].bytes))
	}
	remainingBytes := int64(0)
	for i := n; i < len(c.sl); i++ {
		remainingBytes += c.sl[i].bytes
	}
	if remainingBytes > 0 {
		buf.Printf(", + %s more bytes across %d streams",
			humanizeutil.IBytes(remainingBytes), len(c.sl)-n)
	}
}
