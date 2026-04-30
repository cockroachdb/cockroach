// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// logEngineMetrics holds a subset of storage engine metrics for the separate
// LogEngine. These metrics are only populated when the store uses separated
// engines (i.e. an experimental log engine distinct from the state engine).
// When engines are not separated, these remain at zero.
type logEngineMetrics struct {
	// LSM health.
	L0Sublevels       *metric.Gauge
	L0NumFiles        *metric.Gauge
	NumSSTables       *metric.Gauge
	ReadAmplification *metric.Gauge
	PendingCompaction *metric.Gauge
	LevelSize         [7]*metric.Gauge

	// Write path.
	MemtableTotalSize     *metric.Gauge
	Flushes               *metric.Counter
	FlushedBytes          *metric.Counter
	Compactions           *metric.Counter
	CompactedBytesRead    *metric.Counter
	CompactedBytesWritten *metric.Counter
	WriteStalls           *metric.Counter
	WriteStallNanos       *metric.Counter
	WriteAmplification    *metric.GaugeFloat64

	// WAL.
	WALBytesWritten *metric.Counter
	WALBytesIn      *metric.Counter

	// Batch commits.
	BatchCommitCount              *metric.Counter
	BatchCommitDuration           *metric.Counter
	BatchCommitSemWaitDuration    *metric.Counter
	BatchCommitWALQWaitDuration   *metric.Counter
	BatchCommitMemStallDuration   *metric.Counter
	BatchCommitL0StallDuration    *metric.Counter
	BatchCommitWALRotWaitDuration *metric.Counter
	BatchCommitCommitWaitDuration *metric.Counter
}

var (
	metaLogEngineL0Sublevels = metric.Metadata{
		Name:        "log-engine.l0-sublevels",
		Help:        "Number of L0 sublevels in the log engine's LSM",
		Measurement: "Sublevels",
		Unit:        metric.Unit_COUNT,
	}
	metaLogEngineL0NumFiles = metric.Metadata{
		Name:        "log-engine.l0-num-files",
		Help:        "Number of L0 files in the log engine's LSM",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
	}
	metaLogEngineNumSSTables = metric.Metadata{
		Name:        "log-engine.num-sstables",
		Help:        "Number of SSTables in the log engine",
		Measurement: "SSTables",
		Unit:        metric.Unit_COUNT,
	}
	metaLogEngineReadAmplification = metric.Metadata{
		Name:        "log-engine.read-amplification",
		Help:        "Read amplification in the log engine",
		Measurement: "Disk Reads per Query",
		Unit:        metric.Unit_CONST,
	}
	metaLogEnginePendingCompaction = metric.Metadata{
		Name:        "log-engine.pending-compaction",
		Help:        "Estimated bytes pending compaction in the log engine",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaLogEngineLevelSize = logEngineLevelMetadata(
		"level-size",
		"Size of the SSTables in log engine level %d",
		"Bytes",
		metric.Unit_BYTES,
	)
	metaLogEngineMemtableTotalSize = metric.Metadata{
		Name:        "log-engine.memtable.total-size",
		Help:        "Current size of memtable in the log engine",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaLogEngineFlushes = metric.Metadata{
		Name:        "log-engine.flushes",
		Help:        "Number of flushes in the log engine",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaLogEngineFlushedBytes = metric.Metadata{
		Name:        "log-engine.flushed-bytes",
		Help:        "Bytes written during flush in the log engine",
		Measurement: "Bytes Written",
		Unit:        metric.Unit_BYTES,
	}
	metaLogEngineCompactions = metric.Metadata{
		Name:        "log-engine.compactions",
		Help:        "Number of compactions in the log engine",
		Measurement: "Compactions",
		Unit:        metric.Unit_COUNT,
	}
	metaLogEngineCompactedBytesRead = metric.Metadata{
		Name:        "log-engine.compacted-bytes-read",
		Help:        "Bytes read during compaction in the log engine",
		Measurement: "Bytes Read",
		Unit:        metric.Unit_BYTES,
	}
	metaLogEngineCompactedBytesWritten = metric.Metadata{
		Name:        "log-engine.compacted-bytes-written",
		Help:        "Bytes written during compaction in the log engine",
		Measurement: "Bytes Written",
		Unit:        metric.Unit_BYTES,
	}
	metaLogEngineWriteStalls = metric.Metadata{
		Name:        "log-engine.write-stalls",
		Help:        "Number of write stalls in the log engine",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaLogEngineWriteStallNanos = metric.Metadata{
		Name:        "log-engine.write-stall-nanos",
		Help:        "Total duration of write stalls in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaLogEngineWriteAmplification = metric.Metadata{
		Name:        "log-engine.write-amplification",
		Help:        "Write amplification in the log engine",
		Measurement: "Amplification",
		Unit:        metric.Unit_CONST,
	}
	metaLogEngineWALBytesWritten = metric.Metadata{
		Name:        "log-engine.wal-bytes-written",
		Help:        "Bytes written to the WAL in the log engine",
		Measurement: "Bytes Written",
		Unit:        metric.Unit_BYTES,
	}
	metaLogEngineWALBytesIn = metric.Metadata{
		Name:        "log-engine.wal-bytes-in",
		Help:        "Logical bytes written to the WAL in the log engine",
		Measurement: "Bytes Written",
		Unit:        metric.Unit_BYTES,
	}
	metaLogEngineBatchCommitCount = metric.Metadata{
		Name:        "log-engine.batch-commit.count",
		Help:        "Number of batch commits in the log engine",
		Measurement: "Commits",
		Unit:        metric.Unit_COUNT,
	}
	metaLogEngineBatchCommitDuration = metric.Metadata{
		Name:        "log-engine.batch-commit.duration",
		Help:        "Total duration of batch commits in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaLogEngineBatchCommitSemWaitDuration = metric.Metadata{
		Name:        "log-engine.batch-commit.sem-wait-duration",
		Help:        "Total time spent waiting on the commit semaphore in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaLogEngineBatchCommitWALQWaitDuration = metric.Metadata{
		Name:        "log-engine.batch-commit.wal-queue-wait-duration",
		Help:        "Total time spent waiting on the WAL queue in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaLogEngineBatchCommitMemStallDuration = metric.Metadata{
		Name:        "log-engine.batch-commit.mem-stall-duration",
		Help:        "Total time spent in memtable write stalls during batch commits in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaLogEngineBatchCommitL0StallDuration = metric.Metadata{
		Name:        "log-engine.batch-commit.l0-stall-duration",
		Help:        "Total time spent in L0 read-amp write stalls during batch commits in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaLogEngineBatchCommitWALRotWaitDuration = metric.Metadata{
		Name:        "log-engine.batch-commit.wal-rotation-duration",
		Help:        "Total time spent waiting for WAL rotation in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaLogEngineBatchCommitCommitWaitDuration = metric.Metadata{
		Name:        "log-engine.batch-commit.commit-wait-duration",
		Help:        "Total time spent waiting for commit in the log engine",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

func logEngineLevelMetadata(
	name, helpTpl, measurement string, unit metric.Unit,
) [7]metric.Metadata {
	var sl [7]metric.Metadata
	for i := range sl {
		sl[i] = metric.Metadata{
			Name:        fmt.Sprintf("log-engine.l%d-%s", i, name),
			Help:        fmt.Sprintf(helpTpl, i),
			Measurement: measurement,
			Unit:        unit,
		}
	}
	return sl
}

func newLogEngineMetrics() logEngineMetrics {
	var levelSize [7]*metric.Gauge
	for i := range levelSize {
		levelSize[i] = metric.NewGauge(metaLogEngineLevelSize[i])
	}
	return logEngineMetrics{
		L0Sublevels:       metric.NewGauge(metaLogEngineL0Sublevels),
		L0NumFiles:        metric.NewGauge(metaLogEngineL0NumFiles),
		NumSSTables:       metric.NewGauge(metaLogEngineNumSSTables),
		ReadAmplification: metric.NewGauge(metaLogEngineReadAmplification),
		PendingCompaction: metric.NewGauge(metaLogEnginePendingCompaction),
		LevelSize:         levelSize,

		MemtableTotalSize:     metric.NewGauge(metaLogEngineMemtableTotalSize),
		Flushes:               metric.NewCounter(metaLogEngineFlushes),
		FlushedBytes:          metric.NewCounter(metaLogEngineFlushedBytes),
		Compactions:           metric.NewCounter(metaLogEngineCompactions),
		CompactedBytesRead:    metric.NewCounter(metaLogEngineCompactedBytesRead),
		CompactedBytesWritten: metric.NewCounter(metaLogEngineCompactedBytesWritten),
		WriteStalls:           metric.NewCounter(metaLogEngineWriteStalls),
		WriteStallNanos:       metric.NewCounter(metaLogEngineWriteStallNanos),
		WriteAmplification:    metric.NewGaugeFloat64(metaLogEngineWriteAmplification),

		WALBytesWritten: metric.NewCounter(metaLogEngineWALBytesWritten),
		WALBytesIn:      metric.NewCounter(metaLogEngineWALBytesIn),

		BatchCommitCount:              metric.NewCounter(metaLogEngineBatchCommitCount),
		BatchCommitDuration:           metric.NewCounter(metaLogEngineBatchCommitDuration),
		BatchCommitSemWaitDuration:    metric.NewCounter(metaLogEngineBatchCommitSemWaitDuration),
		BatchCommitWALQWaitDuration:   metric.NewCounter(metaLogEngineBatchCommitWALQWaitDuration),
		BatchCommitMemStallDuration:   metric.NewCounter(metaLogEngineBatchCommitMemStallDuration),
		BatchCommitL0StallDuration:    metric.NewCounter(metaLogEngineBatchCommitL0StallDuration),
		BatchCommitWALRotWaitDuration: metric.NewCounter(metaLogEngineBatchCommitWALRotWaitDuration),
		BatchCommitCommitWaitDuration: metric.NewCounter(metaLogEngineBatchCommitCommitWaitDuration),
	}
}

func (lm *logEngineMetrics) update(m storage.Metrics) {
	lm.L0Sublevels.Update(int64(m.Levels[0].Sublevels))
	lm.L0NumFiles.Update(int64(m.Levels[0].Tables.Count))
	lm.NumSSTables.Update(m.NumSSTables())
	lm.ReadAmplification.Update(int64(m.ReadAmp()))
	lm.PendingCompaction.Update(int64(m.Compact.EstimatedDebt))

	lm.MemtableTotalSize.Update(int64(m.MemTable.Size))
	lm.Flushes.Update(m.Flush.Count)
	lm.FlushedBytes.Update(int64(m.Levels[0].TablesFlushed.Bytes + m.Levels[0].BlobBytesFlushed))
	lm.Compactions.Update(m.Compact.Count)
	compactedRead, compactedWritten := m.CompactedBytes()
	lm.CompactedBytesRead.Update(int64(compactedRead))
	lm.CompactedBytesWritten.Update(int64(compactedWritten))
	lm.WriteStalls.Update(m.WriteStallCount)
	lm.WriteStallNanos.Update(m.WriteStallDuration.Nanoseconds())

	// NB: `UpdateIfHigher` is used here since there is a race in pebble where
	// sometimes the WAL is rotated but metrics are retrieved prior to the update
	// to BytesIn to account for the previous WAL.
	lm.WALBytesWritten.UpdateIfHigher(int64(m.WAL.BytesWritten))
	lm.WALBytesIn.Update(int64(m.WAL.BytesIn))

	lm.BatchCommitCount.Update(int64(m.BatchCommitStats.Count))
	lm.BatchCommitDuration.Update(int64(m.BatchCommitStats.TotalDuration))
	lm.BatchCommitSemWaitDuration.Update(int64(m.BatchCommitStats.SemaphoreWaitDuration))
	lm.BatchCommitWALQWaitDuration.Update(int64(m.BatchCommitStats.WALQueueWaitDuration))
	lm.BatchCommitMemStallDuration.Update(int64(m.BatchCommitStats.MemTableWriteStallDuration))
	lm.BatchCommitL0StallDuration.Update(int64(m.BatchCommitStats.L0ReadAmpWriteStallDuration))
	lm.BatchCommitWALRotWaitDuration.Update(int64(m.BatchCommitStats.WALRotationDuration))
	lm.BatchCommitCommitWaitDuration.Update(int64(m.BatchCommitStats.CommitWaitDuration))

	for level, stats := range m.Levels {
		lm.LevelSize[level].Update(int64(stats.Tables.Bytes))
	}
	tot := m.Total()
	lm.WriteAmplification.Update(tot.WriteAmp())
}
