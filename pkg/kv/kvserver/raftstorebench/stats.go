// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func statsLoop(t T, ctx context.Context, cfg Config, s *aggStats, raftEng, smEng storage.Engine) {
	const d = 10 * time.Second
	var raftTmr timeutil.Timer
	if cfg.RaftMetricsInterval > 0 {
		raftTmr.Reset(cfg.RaftMetricsInterval)
	}
	var tmr timeutil.Timer
	tmr.Reset(d)
	for {
		var last bool
		var raft bool
		select {
		case <-tmr.C:
			tmr.Reset(d)
		case <-raftTmr.C:
			raftTmr.Reset(cfg.RaftMetricsInterval)
			raft = true
		case <-ctx.Done():
			last = true
		}

		i := s.ops.Load()
		kb := s.keyBytes.Load()
		vb := s.valBytes.Load()

		if raft {
			m := raftEng.GetMetrics()
			was := writeAmp(m, kb+vb)
			logf(t, "raft lsm:\n%s", &m)
			logf(t, "raft w-amp: %s", &was)
			continue
		}

		logf(t, "ops=%d (%d%%) payload(total,keys,values)=(%s,%s,%s) truncs=%d",
			i, int(100*float64(i)/float64(cfg.NumWrites)),
			humanizeutil.IBytes(kb+vb), humanizeutil.IBytes(kb), humanizeutil.IBytes(vb),
			s.truncs.Load())
		if cfg.WALMetrics {
			raftMetrics := raftEng.GetMetrics()
			if cfg.SingleEngine {
				printLogWriterMetrics(t, "joint", raftMetrics)
			} else {
				printLogWriterMetrics(t, "raft ", raftMetrics)
				if !cfg.SMDisableWAL {
					smMetrics := smEng.GetMetrics()
					printLogWriterMetrics(t, "state", smMetrics)
				}
			}
		}

		if last {
			return
		}
	}
}

type aggStats struct {
	ops      atomic.Int64
	keyBytes atomic.Int64
	valBytes atomic.Int64
	truncs   atomic.Int64 // # truncations (across all repls)
}

func newAggStats() *aggStats {
	return &aggStats{}
}

func printLogWriterMetrics(t T, prefix string, m storage.Metrics) {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "%s logWriter: rate(peak): %s(%s) len(pending,sync): (%.2f,%.2f)",
		prefix, humanize.IBytes(uint64(m.LogWriter.WriteThroughput.Rate())),
		humanize.IBytes(uint64(m.LogWriter.WriteThroughput.PeakRate())),
		m.LogWriter.PendingBufferLen.Mean(), m.LogWriter.SyncQueueLen.Mean())
	qs := calculateQuantiles(m.LogWriter.FsyncLatency, 0.5, 0.9, 0.99)
	_, _ = fmt.Fprintf(&b, " sync: p50,p90,p99: %.2fms,%.2fms,%.2fms", qs[0]/1e6, qs[1]/1e6, qs[2]/1e6)
	logf(t, "%s", &b)
}

func calculateQuantiles(h prometheus.Histogram, sortedQuantiles ...float64) []float64 {
	metric := &io_prometheus_client.Metric{}
	if err := h.Write(metric); err != nil {
		panic(err)
	}

	buckets := metric.GetHistogram().GetBucket()
	totalCount := metric.GetHistogram().GetSampleCount()
	targetRank := uint64(float64(totalCount) * sortedQuantiles[0])

	var outputs []float64
	var cumulativeCount uint64
	for _, bucket := range buckets {
		cumulativeCount += bucket.GetCumulativeCount()
		for cumulativeCount >= targetRank {
			outputs = append(outputs, bucket.GetUpperBound()) // approx
			sortedQuantiles = sortedQuantiles[1:]
			if len(sortedQuantiles) == 0 {
				return outputs
			}
			targetRank = uint64(float64(totalCount) * sortedQuantiles[0])
		}
	}
	panic(outputs)
}

type WriteAmpStats struct {
	PayloadBytes int64

	WALBytesWritten        int64
	CompactionBytesWritten int64 // flush+compactions
	FlushBytesWritten      int64 // flushes only

	Human HumanWriteAmpStats `yaml:",omitempty"`
}

type HumanWriteAmpStats struct {
	// Rounded from WriteAmpStats.
	PayloadGB    float64 `yaml:",omitempty"`
	WALGB        float64
	CompactionGB float64 // flush+compactions
	FlushGB      float64 // flushes only

	// Computed from WriteAmpStats.
	//
	// W: WALBytesWritten
	// C: CompactionBytesWritten
	// F: FlushBytesWritten
	// P: PayloadBytes

	// WriteAmp expresses the total disk bytes (WAL plus any LSM activity)
	// as a multiple of the payload. For example, for the raft engine, in a
	// perfect world  this is just slightly above 1 as every entry is
	// written once to the WAL but truncated before flushing to the LSM. On
	// the state machine LSM under uniform writes, we expect much larger
	// numbers (multiple dozen).
	WriteAmp float64 // (W+C)/P
	// FlushFrac expresses the size of the WAL as a multiple of how much was
	// flushed. For example, for the raft engine, if all raft entries are
	// truncated before the memtable got flushed (i.e. the ideal state), the
	// flush percentage would be close to zero.
	// NB: L0 is compressed, memtables are not, so even if everything is
	// flushed this can be well below 1.
	FlushFrac float64 // F/W
}

func (was *WriteAmpStats) String() string {
	return fmt.Sprintf("W-Amp=%.2f=(W+C)/P FlushFrac=%.2f=F/W "+
		"[P]ayload=%.2fGiB [W]AL=%.2fGiB [F]lush=%.2fGiB [C]ompaction=%.2fGiB",
		was.Human.WriteAmp, was.Human.FlushFrac,
		was.Human.PayloadGB, was.Human.WALGB, was.Human.FlushGB, was.Human.CompactionGB,
	)
}

func writeAmp(m storage.Metrics, payloadBytes int64) WriteAmpStats {
	var was WriteAmpStats
	was.WALBytesWritten += int64(m.WAL.BytesWritten)
	for i := 0; i < len(m.Levels); i++ {
		flushed := m.Levels[i].TableBytesFlushed + m.Levels[i].BlobBytesFlushed // only populated for L0
		compacted := m.Levels[i].TableBytesCompacted + m.Levels[i].BlobBytesCompacted
		was.FlushBytesWritten += int64(flushed)
		was.CompactionBytesWritten += int64(flushed + compacted)
	}
	was.PayloadBytes = payloadBytes

	was.initHuman()

	return was
}

func (was *WriteAmpStats) initHuman() {
	was.Human.PayloadGB = float64(was.PayloadBytes) / 1e9
	was.Human.WALGB = float64(was.WALBytesWritten) / 1e9
	was.Human.CompactionGB = float64(was.CompactionBytesWritten) / 1e9
	was.Human.FlushGB = float64(was.FlushBytesWritten) / 1e9
	was.Human.WriteAmp = float64(was.CompactionBytesWritten+was.WALBytesWritten) / float64(was.PayloadBytes)
	was.Human.FlushFrac = float64(was.FlushBytesWritten) / float64(was.WALBytesWritten)
}

func (was *WriteAmpStats) add(other WriteAmpStats) WriteAmpStats {
	p := was.PayloadBytes
	if other.PayloadBytes > p {
		p = other.PayloadBytes
	}
	out := WriteAmpStats{
		PayloadBytes:           p,
		WALBytesWritten:        was.WALBytesWritten + other.WALBytesWritten,
		CompactionBytesWritten: was.CompactionBytesWritten + other.CompactionBytesWritten,
		FlushBytesWritten:      was.FlushBytesWritten + other.FlushBytesWritten,
	}
	out.initHuman()
	return out
}

type Result struct {
	Name                           string
	PayloadGB                      float64
	DurationSec                    float64
	GoodputMB                      float64
	CombinedEng, StateEng, RaftEng HumanWriteAmpStats `yaml:",omitempty"`
	Truncations                    int64
}

func logf(t T, format string, args ...any) {
	t.Helper()
	// I250218 16:28:53.798676 13 3@pebble/event.go:985  [s?,pebble] 331  [JOB 79] sstable deleted 000049
	t.Logf(timeutil.Now().Format("15:04:05 ")+format, args...)
}
