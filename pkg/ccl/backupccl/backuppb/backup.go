// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuppb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	_ "github.com/cockroachdb/cockroach/pkg/util/uuid" // required for backup.proto
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/jsonpb"
	"go.opentelemetry.io/otel/attribute"
)

// IsIncremental returns if the BackupManifest corresponds to an incremental
// backup.
func (m *BackupManifest) IsIncremental() bool {
	return !m.StartTime.IsEmpty()
}

// GetTenants retrieves the tenant information from the manifest. It should be
// used instead of Tenants to support older versions of the manifest which used
// the deprecated field.
func (m *BackupManifest) GetTenants() []descpb.TenantInfoWithUsage {
	if len(m.Tenants) > 0 {
		return m.Tenants
	}
	if len(m.TenantsDeprecated) > 0 {
		res := make([]descpb.TenantInfoWithUsage, len(m.TenantsDeprecated))
		for i := range res {
			res[i].TenantInfo = m.TenantsDeprecated[i]
		}
		return res
	}
	return nil
}

// HasTenants returns true if the manifest contains (non-system) tenant data.
func (m *BackupManifest) HasTenants() bool {
	return len(m.Tenants) > 0 || len(m.TenantsDeprecated) > 0
}

// MarshalJSONPB implements jsonpb.JSONPBMarshaller to provide a custom Marshaller
// for jsonpb that redacts secrets in URI fields.
func (m ScheduledBackupExecutionArgs) MarshalJSONPB(marshaller *jsonpb.Marshaler) ([]byte, error) {
	if !protoreflect.ShouldRedact(marshaller) {
		return json.Marshal(m)
	}

	stmt, err := parser.ParseOne(m.BackupStatement)
	if err != nil {
		return nil, err
	}
	backup, ok := stmt.AST.(*tree.Backup)
	if !ok {
		return nil, errors.Errorf("unexpected %T statement in backup schedule: %v", backup, backup)
	}

	for i := range backup.To {
		raw, ok := backup.To[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.To[i] = tree.NewDString(clean)
	}

	// NB: this will never be non-nil with current schedule syntax but is here for
	// completeness.
	for i := range backup.IncrementalFrom {
		raw, ok := backup.IncrementalFrom[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.IncrementalFrom[i] = tree.NewDString(clean)
	}

	for i := range backup.Options.IncrementalStorage {
		raw, ok := backup.Options.IncrementalStorage[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.Options.IncrementalStorage[i] = tree.NewDString(clean)
	}

	for i := range backup.Options.EncryptionKMSURI {
		raw, ok := backup.Options.EncryptionKMSURI[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.RedactKMSURI(raw.RawString())
		if err != nil {
			return nil, err
		}
		backup.Options.EncryptionKMSURI[i] = tree.NewDString(clean)
	}

	if backup.Options.EncryptionPassphrase != nil {
		backup.Options.EncryptionPassphrase = tree.NewDString("redacted")
	}

	m.BackupStatement = backup.String()
	return json.Marshal(m)
}

var _ bulk.TracingAggregatorEvent = &ExportStats{}

// Render implements the LazyTag interface.
func (e *ExportStats) Render() []attribute.KeyValue {
	const mb = 1 << 20
	tags := make([]attribute.KeyValue, 0)
	if e.NumFiles > 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   "num_files",
			Value: attribute.Int64Value(e.NumFiles),
		})
	}
	if e.DataSize > 0 {
		dataSizeMB := float64(e.DataSize) / mb
		tags = append(tags, attribute.KeyValue{
			Key:   "data_size",
			Value: attribute.StringValue(fmt.Sprintf("%.2f MB", dataSizeMB)),
		})

		if e.Duration > 0 {
			throughput := dataSizeMB / e.Duration.Seconds()
			tags = append(tags, attribute.KeyValue{
				Key:   "throughput",
				Value: attribute.StringValue(fmt.Sprintf("%.2f MB/s", throughput)),
			})
		}
	}

	return tags
}

// Identity implements the TracingAggregatorEvent interface.
func (e *ExportStats) Identity() bulk.TracingAggregatorEvent {
	return &ExportStats{}
}

// Combine implements the TracingAggregatorEvent interface.
func (e *ExportStats) Combine(other bulk.TracingAggregatorEvent) {
	otherExportStats, ok := other.(*ExportStats)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type ExportStats: %T", other))
	}
	e.NumFiles += otherExportStats.NumFiles
	e.DataSize += otherExportStats.DataSize
	e.Duration += otherExportStats.Duration
}

// Tag implements the TracingAggregatorEvent interface.
func (e *ExportStats) Tag() string {
	return "ExportStats"
}

// Identity implements the TracingAggregatorEvent interface.
func (s *IngestionPerformanceStats) Identity() bulk.TracingAggregatorEvent {
	stats := IngestionPerformanceStats{}
	stats.SendWaitByStore = make(map[roachpb.StoreID]time.Duration)
	return &stats
}

// Combine implements the TracingAggregatorEvent interface.
func (s *IngestionPerformanceStats) Combine(other bulk.TracingAggregatorEvent) {
	otherStats, ok := other.(*IngestionPerformanceStats)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type IngestionPerformanceStats: %T", other))
	}

	s.DataSize += otherStats.DataSize
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
	s.Duration += otherStats.Duration

	for k, v := range otherStats.SendWaitByStore {
		s.SendWaitByStore[k] += v
	}
}

// Tag implements the TracingAggregatorEvent interface.
func (s *IngestionPerformanceStats) Tag() string {
	return "IngestionPerformanceStats"
}

// Render implements the TracingAggregatorEvent interface.
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

	if s.DataSize > 0 {
		dataSizeMB := float64(s.DataSize) / mb
		tags = append(tags, attribute.KeyValue{
			Key:   "data_size",
			Value: attribute.StringValue(fmt.Sprintf("%.2f MB", dataSizeMB)),
		})

		if s.Duration > 0 {
			throughput := dataSizeMB / s.Duration.Seconds()
			tags = append(tags, attribute.KeyValue{
				Key:   "throughput",
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

// LogTimings logs the timing ingestion stats.
func (s *IngestionPerformanceStats) LogTimings(ctx context.Context, name, action string) {
	log.Infof(ctx,
		"%s adder %s; ingested %s: %s filling; %v sorting; %v / %v flushing; %v sending; %v splitting; %d; %v scattering, %d, %v; %v commit-wait",
		name,
		redact.Safe(action),
		sz(s.DataSize),
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

var _ bulk.TracingAggregatorEvent = &IngestionPerformanceStats{}

func init() {
	protoreflect.RegisterShorthands((*BackupManifest)(nil), "backup", "backup_manifest")
}
