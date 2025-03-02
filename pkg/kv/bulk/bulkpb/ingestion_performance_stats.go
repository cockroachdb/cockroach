// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkpb

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/redact"
	"github.com/codahale/hdrhistogram"
	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/otel/attribute"
)

var _ tracing.AggregatorEvent = (*IngestionPerformanceStats)(nil)

const (
	sigFigs    = 1
	minLatency = time.Millisecond
	maxLatency = 100 * time.Second

	minBytes = 1024              // 1 KB
	maxBytes = 256 * 1024 * 1024 // 256 MB
)

// Identity implements the AggregatorEvent interface.
func (s *IngestionPerformanceStats) Identity() tracing.AggregatorEvent {
	stats := IngestionPerformanceStats{
		LastFlushTime:    hlc.Timestamp{WallTime: math.MaxInt64},
		CurrentFlushTime: hlc.Timestamp{WallTime: math.MinInt64},
	}
	stats.SendWaitByStore = make(map[roachpb.StoreID]time.Duration)
	return &stats
}

// getCombinedHist returns a new HistogramData that contains the currentHist
// combined with the recordValue. If currentHist is nil, a new histogram is
// initialized.
func getCombinedHist(
	currentHist *HistogramData, recordValue int64, dataType HistogramDataType,
) *HistogramData {
	var hist *hdrhistogram.Histogram
	if currentHist != nil {
		hist = hdrhistogram.Import(&hdrhistogram.Snapshot{
			LowestTrackableValue:  currentHist.LowestTrackableValue,
			HighestTrackableValue: currentHist.HighestTrackableValue,
			SignificantFigures:    currentHist.SignificantFigures,
			Counts:                currentHist.Counts,
		})
	} else if dataType == HistogramDataTypeLatency {
		hist = hdrhistogram.New(minLatency.Nanoseconds(),
			maxLatency.Nanoseconds(), sigFigs)
	} else if dataType == HistogramDataTypeBytes {
		hist = hdrhistogram.New(minBytes, maxBytes, sigFigs)
	}
	_ = hist.RecordValue(recordValue)
	// Return the snapshot of this new merged histogram.
	cumulativeSnapshot := hist.Export()
	return &HistogramData{
		DataType:              dataType,
		LowestTrackableValue:  cumulativeSnapshot.LowestTrackableValue,
		HighestTrackableValue: cumulativeSnapshot.HighestTrackableValue,
		SignificantFigures:    cumulativeSnapshot.SignificantFigures,
		Counts:                cumulativeSnapshot.Counts,
	}
}

// Combine implements the AggregatorEvent interface.
func (s *IngestionPerformanceStats) Combine(other tracing.AggregatorEvent) {
	otherStats, ok := other.(*IngestionPerformanceStats)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type IngestionPerformanceStats: %T", other))
	}

	s.LogicalDataSize += otherStats.LogicalDataSize
	s.SSTDataSize += otherStats.SSTDataSize
	s.BufferFlushes += otherStats.BufferFlushes
	s.FlushesDueToSize += otherStats.FlushesDueToSize
	s.Batches += otherStats.Batches
	s.BatchesDueToRange += otherStats.BatchesDueToRange
	s.BatchesDueToSize += otherStats.BatchesDueToSize
	s.SplitRetries += otherStats.SplitRetries
	s.Splits += otherStats.Splits
	s.Scatters += otherStats.Scatters
	s.ScatterMoved += otherStats.ScatterMoved
	s.FillWait += otherStats.FillWait
	s.SortWait += otherStats.SortWait
	s.FlushWait += otherStats.FlushWait
	s.BatchWait += otherStats.BatchWait
	s.SendWait += otherStats.SendWait
	s.SplitWait += otherStats.SplitWait
	s.ScatterWait += otherStats.ScatterWait
	s.CommitWait += otherStats.CommitWait
	s.AsWrites += otherStats.AsWrites

	s.BatchWaitHist = getCombinedHist(s.BatchWaitHist,
		otherStats.BatchWait.Nanoseconds(), HistogramDataTypeLatency)
	s.SstSizeHist = getCombinedHist(s.SstSizeHist,
		otherStats.SSTDataSize, HistogramDataTypeBytes)

	// Duration should not be used in throughput calculations as adding durations
	// of multiple flushes does not account for concurrent execution of these
	// flushes.
	s.Duration += otherStats.Duration

	// We want to store the earliest of the FlushTimes.
	if otherStats.LastFlushTime.Less(s.LastFlushTime) {
		s.LastFlushTime = otherStats.LastFlushTime
	}

	// We want to store the latest of the FlushTimes.
	if s.CurrentFlushTime.Less(otherStats.CurrentFlushTime) {
		s.CurrentFlushTime = otherStats.CurrentFlushTime
	}

	for k, v := range otherStats.SendWaitByStore {
		s.SendWaitByStore[k] += v
	}
}

// ProtoName implements the AggregatorEvent interface.
func (s *IngestionPerformanceStats) ProtoName() string {
	return proto.MessageName(s)
}

func (s *IngestionPerformanceStats) ToText() []byte {
	return []byte(s.String())
}

// String implements the stringer interface.
func (s *IngestionPerformanceStats) String() string {
	const mb = 1 << 20
	var b strings.Builder
	if s.Batches > 0 {
		b.WriteString(fmt.Sprintf("num_batches: %d\n", s.Batches))
		b.WriteString(fmt.Sprintf("num_batches_due_to_size: %d\n", s.BatchesDueToSize))
		b.WriteString(fmt.Sprintf("num_batches_due_to_range: %d\n", s.BatchesDueToRange))
		b.WriteString(fmt.Sprintf("split_retries: %d\n", s.SplitRetries))
	}

	if s.BufferFlushes > 0 {
		b.WriteString(fmt.Sprintf("num_flushes: %d\n", s.BufferFlushes))
		b.WriteString(fmt.Sprintf("num_flushes_due_to_size: %d\n", s.FlushesDueToSize))
	}

	if s.LogicalDataSize > 0 {
		logicalDataSizeMB := float64(s.LogicalDataSize) / mb
		b.WriteString(fmt.Sprintf("logical_data_size: %.2f MB\n", logicalDataSizeMB))

		if !s.CurrentFlushTime.IsEmpty() && !s.LastFlushTime.IsEmpty() {
			duration := s.CurrentFlushTime.GoTime().Sub(s.LastFlushTime.GoTime())
			throughput := logicalDataSizeMB / duration.Seconds()
			b.WriteString(fmt.Sprintf("logical_throughput: %.2f MB/s\n", throughput))
		}
	}

	if s.SSTDataSize > 0 {
		sstDataSizeMB := float64(s.SSTDataSize) / mb
		b.WriteString(fmt.Sprintf("sst_data_size: %.2f MB\n", sstDataSizeMB))
		b.WriteString(fmt.Sprintf("sst_data_hist:\n%s\n", s.SstSizeHist.String()))

		if !s.CurrentFlushTime.IsEmpty() && !s.LastFlushTime.IsEmpty() {
			duration := s.CurrentFlushTime.GoTime().Sub(s.LastFlushTime.GoTime())
			throughput := sstDataSizeMB / duration.Seconds()
			b.WriteString(fmt.Sprintf("sst_throughput: %.2f MB/s\n", throughput))
		}
	}

	timeString(&b, "fill_wait", s.FillWait)
	timeString(&b, "sort_wait", s.SortWait)
	timeString(&b, "flush_wait", s.FlushWait)
	timeString(&b, "batch_wait", s.BatchWait)
	b.WriteString(fmt.Sprintf("batch_wait_hist:\n%s\n", s.BatchWaitHist.String()))
	timeString(&b, "send_wait", s.SendWait)
	timeString(&b, "split_wait", s.SplitWait)
	timeString(&b, "scatter_wait", s.ScatterWait)
	timeString(&b, "commit_wait", s.CommitWait)

	b.WriteString(fmt.Sprintf("splits: %d\n", s.Splits))
	b.WriteString(fmt.Sprintf("scatters: %d\n", s.Scatters))
	b.WriteString(fmt.Sprintf("scatter_moved: %d\n", s.ScatterMoved))
	b.WriteString(fmt.Sprintf("as_writes: %d\n", s.AsWrites))

	// Sort store send wait by IDs before adding them as tags.
	ids := make(roachpb.StoreIDSlice, 0, len(s.SendWaitByStore))
	for i := range s.SendWaitByStore {
		ids = append(ids, i)
	}
	sort.Sort(ids)
	for _, id := range ids {
		timeString(&b, fmt.Sprintf("store-%d_send_wait", id), s.SendWaitByStore[id])
	}

	return b.String()
}

// Render implements the AggregatorEvent interface.
func (s *IngestionPerformanceStats) Render() []attribute.KeyValue {
	const mb = 1 << 20
	tags := make([]attribute.KeyValue, 0)
	if s.Batches > 0 {
		tags = append(tags,
			attribute.KeyValue{
				Key:   "num_batches",
				Value: attribute.Int64Value(s.Batches),
			},
			attribute.KeyValue{
				Key:   "num_batches_due_to_size",
				Value: attribute.Int64Value(s.BatchesDueToSize),
			},
			attribute.KeyValue{
				Key:   "num_batches_due_to_range",
				Value: attribute.Int64Value(s.BatchesDueToRange),
			},
			attribute.KeyValue{
				Key:   "split_retires",
				Value: attribute.Int64Value(s.SplitRetries),
			},
		)
	}

	if s.BufferFlushes > 0 {
		tags = append(tags,
			attribute.KeyValue{
				Key:   "num_flushes",
				Value: attribute.Int64Value(s.BufferFlushes),
			},
			attribute.KeyValue{
				Key:   "num_flushes_due_to_size",
				Value: attribute.Int64Value(s.FlushesDueToSize),
			},
		)
	}

	if s.LogicalDataSize > 0 {
		logicalDataSizeMB := float64(s.LogicalDataSize) / mb
		tags = append(tags, attribute.KeyValue{
			Key:   "logical_data_size",
			Value: attribute.StringValue(fmt.Sprintf("%.2f MB", logicalDataSizeMB)),
		})

		if !s.CurrentFlushTime.IsEmpty() && !s.LastFlushTime.IsEmpty() {
			duration := s.CurrentFlushTime.GoTime().Sub(s.LastFlushTime.GoTime())
			throughput := logicalDataSizeMB / duration.Seconds()
			tags = append(tags, attribute.KeyValue{
				Key:   "logical_throughput",
				Value: attribute.StringValue(fmt.Sprintf("%.2f MB/s", throughput)),
			})
		}
	}

	if s.SSTDataSize > 0 {
		sstDataSizeMB := float64(s.SSTDataSize) / mb
		tags = append(tags, attribute.KeyValue{
			Key:   "sst_data_size",
			Value: attribute.StringValue(fmt.Sprintf("%.2f MB", sstDataSizeMB)),
		})

		if !s.CurrentFlushTime.IsEmpty() && !s.LastFlushTime.IsEmpty() {
			duration := s.CurrentFlushTime.GoTime().Sub(s.LastFlushTime.GoTime())
			throughput := sstDataSizeMB / duration.Seconds()
			tags = append(tags, attribute.KeyValue{
				Key:   "sst_throughput",
				Value: attribute.StringValue(fmt.Sprintf("%.2f MB/s", throughput)),
			})
		}
	}

	tags = append(tags,
		timeKeyValue("fill_wait", s.FillWait),
		timeKeyValue("sort_wait", s.SortWait),
		timeKeyValue("flush_wait", s.FlushWait),
		timeKeyValue("batch_wait", s.BatchWait),
		timeKeyValue("send_wait", s.SendWait),
		timeKeyValue("split_wait", s.SplitWait),
		attribute.KeyValue{Key: "splits", Value: attribute.Int64Value(s.Splits)},
		timeKeyValue("scatter_wait", s.ScatterWait),
		attribute.KeyValue{Key: "scatters", Value: attribute.Int64Value(s.Scatters)},
		attribute.KeyValue{Key: "scatter_moved", Value: attribute.Int64Value(s.ScatterMoved)},
		timeKeyValue("commit_wait", s.CommitWait),
	)

	// Sort store send wait by IDs before adding them as tags.
	ids := make(roachpb.StoreIDSlice, 0, len(s.SendWaitByStore))
	for i := range s.SendWaitByStore {
		ids = append(ids, i)
	}
	sort.Sort(ids)
	for _, id := range ids {
		tags = append(tags, timeKeyValue(attribute.Key(fmt.Sprintf("store-%d_send_wait", id)), s.SendWaitByStore[id]))
	}

	return tags
}

func timeKeyValue(key attribute.Key, time time.Duration) attribute.KeyValue {
	return attribute.KeyValue{
		Key:   key,
		Value: attribute.StringValue(string(humanizeutil.Duration(time))),
	}
}

func timeString(b *strings.Builder, key string, time time.Duration) {
	b.WriteString(fmt.Sprintf("%s: %s\n", key, string(humanizeutil.Duration(time))))
}

// LogTimings logs the timing ingestion stats.
func (s *IngestionPerformanceStats) LogTimings(ctx context.Context, name, action string) {
	log.Infof(ctx,
		"%s adder %s; ingested %s: %s filling; %v sorting; %v / %v flushing; %v sending; %v splitting; %d; %v scattering, %d, %v; %v commit-wait",
		name,
		redact.Safe(action),
		sz(s.LogicalDataSize),
		timing(s.FillWait),
		timing(s.SortWait),
		timing(s.FlushWait),
		timing(s.BatchWait),
		timing(s.SendWait),
		timing(s.SplitWait),
		s.Splits,
		timing(s.ScatterWait),
		s.Scatters,
		s.ScatterMoved,
		timing(s.CommitWait),
	)
}

// LogFlushes logs stats about buffering added and SST batcher flushes.
func (s *IngestionPerformanceStats) LogFlushes(
	ctx context.Context, name, action string, bufSize int64, span roachpb.Span,
) {
	log.Infof(ctx,
		"%s adder %s; flushed into %s %d times, %d due to buffer size (%s); flushing chunked into %d files (%d for ranges, %d for sst size) +%d split-retries",
		name,
		redact.Safe(action),
		span,
		s.BufferFlushes,
		s.FlushesDueToSize,
		sz(bufSize),
		s.Batches,
		s.BatchesDueToRange,
		s.BatchesDueToSize,
		s.SplitRetries,
	)
}

// LogPerStoreTimings logs send waits per store.
func (s *IngestionPerformanceStats) LogPerStoreTimings(ctx context.Context, name string) {
	if len(s.SendWaitByStore) == 0 {
		return
	}
	ids := make(roachpb.StoreIDSlice, 0, len(s.SendWaitByStore))
	for i := range s.SendWaitByStore {
		ids = append(ids, i)
	}
	sort.Sort(ids)

	var sb strings.Builder
	for i, id := range ids {
		// Hack: fill the map with placeholder stores if we haven't seen the store
		// with ID below K for all but lowest K, so that next time we print a zero.
		if i > 0 && ids[i-1] != id-1 {
			s.SendWaitByStore[id-1] = 0
			fmt.Fprintf(&sb, "%d: %s;", id-1, timing(0))
		}
		fmt.Fprintf(&sb, "%d: %s;", id, timing(s.SendWaitByStore[id]))

	}
	log.Infof(ctx, "%s waited on sending to: %s", name, redact.Safe(sb.String()))
}

type sz int64

func (b sz) String() string { return string(humanizeutil.IBytes(int64(b))) }
func (b sz) SafeValue()     {}

type timing time.Duration

func (t timing) String() string { return time.Duration(t).Round(time.Second).String() }
func (t timing) SafeValue()     {}
