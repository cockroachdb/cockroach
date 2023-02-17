// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuppb

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	_ "github.com/cockroachdb/cockroach/pkg/util/uuid" // required for backup.proto
	"github.com/cockroachdb/errors"
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
func (m *BackupManifest) GetTenants() []mtinfopb.TenantInfoWithUsage {
	if len(m.Tenants) > 0 {
		return m.Tenants
	}
	if len(m.TenantsDeprecated) > 0 {
		res := make([]mtinfopb.TenantInfoWithUsage, len(m.TenantsDeprecated))
		for i := range res {
			res[i].ProtoInfo = m.TenantsDeprecated[i]
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

		if !e.StartTime.IsEmpty() && !e.EndTime.IsEmpty() {
			duration := e.EndTime.GoTime().Sub(e.StartTime.GoTime())
			throughput := dataSizeMB / duration.Seconds()
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
	return &ExportStats{
		StartTime: hlc.Timestamp{WallTime: math.MaxInt64},
		EndTime:   hlc.Timestamp{WallTime: math.MinInt64},
	}
}

// Combine implements the TracingAggregatorEvent interface.
func (e *ExportStats) Combine(other bulk.TracingAggregatorEvent) {
	otherExportStats, ok := other.(*ExportStats)
	if !ok {
		panic(fmt.Sprintf("`other` is not of type ExportStats: %T", other))
	}
	e.NumFiles += otherExportStats.NumFiles
	e.DataSize += otherExportStats.DataSize
	// Duration should not be used in throughput calculations as adding durations
	// of two ExportRequests does not account for concurrent evaluation of these
	// requests.
	e.Duration += otherExportStats.Duration

	// We want to store the earliest of the StartTimes.
	if otherExportStats.StartTime.Less(e.StartTime) {
		e.StartTime = otherExportStats.StartTime
	}

	// We want to store the latest of the EndTimes.
	if e.EndTime.Less(otherExportStats.EndTime) {
		e.EndTime = otherExportStats.EndTime
	}
}

// Tag implements the TracingAggregatorEvent interface.
func (e *ExportStats) Tag() string {
	return "ExportStats"
}

func init() {
	protoreflect.RegisterShorthands((*BackupManifest)(nil), "backup", "backup_manifest")
}
