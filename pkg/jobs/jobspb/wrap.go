// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobspb

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// JobID is the ID of a job.
type JobID = catpb.JobID

// InvalidJobID is the zero value for JobID corresponding to no job.
const InvalidJobID = catpb.InvalidJobID

// Details is a marker interface for job details proto structs.
type Details interface{}

var (
	_ Details = BackupDetails{}
	_ Details = RestoreDetails{}
	_ Details = SchemaChangeDetails{}
	_ Details = ChangefeedDetails{}
	_ Details = CreateStatsDetails{}
	_ Details = SchemaChangeGCDetails{}
	_ Details = StreamIngestionDetails{}
	_ Details = NewSchemaChangeDetails{}
	_ Details = MigrationDetails{}
	_ Details = AutoSpanConfigReconciliationDetails{}
	_ Details = ImportDetails{}
	_ Details = StreamReplicationDetails{}
	_ Details = RowLevelTTLDetails{}
	_ Details = SchemaTelemetryDetails{}
	_ Details = KeyVisualizerDetails{}
)

// ProgressDetails is a marker interface for job progress details proto structs.
type ProgressDetails interface{}

var (
	_ ProgressDetails = BackupProgress{}
	_ ProgressDetails = RestoreProgress{}
	_ ProgressDetails = SchemaChangeProgress{}
	_ ProgressDetails = ChangefeedProgress{}
	_ ProgressDetails = CreateStatsProgress{}
	_ ProgressDetails = SchemaChangeGCProgress{}
	_ ProgressDetails = StreamIngestionProgress{}
	_ ProgressDetails = NewSchemaChangeProgress{}
	_ ProgressDetails = MigrationProgress{}
	_ ProgressDetails = AutoSpanConfigReconciliationDetails{}
	_ ProgressDetails = StreamReplicationProgress{}
	_ ProgressDetails = RowLevelTTLProgress{}
	_ ProgressDetails = SchemaTelemetryProgress{}
	_ ProgressDetails = KeyVisualizerProgress{}
)

// Type returns the payload's job type and panics if the type is invalid.
func (p *Payload) Type() Type {
	typ, err := DetailsType(p.Details)
	if err != nil {
		panic(err)
	}
	return typ
}

// CheckType returns the payload's job type with an error
// if the type is invalid.
func (p *Payload) CheckType() (Type, error) {
	return DetailsType(p.Details)
}

// Import base which is in the generated proto field but won't get picked up
// by bazel if it were not imported in a non-generated file.
var _ base.SQLInstanceID

// ReplicationStatus describes the status of the replication stream, and stored
// on the stream ingestion job
type ReplicationStatus uint8

const (
	InitializingReplication   ReplicationStatus = 0
	Replicating               ReplicationStatus = 1
	ReplicationPaused         ReplicationStatus = 2
	ReplicationPendingCutover ReplicationStatus = 3
	ReplicationCuttingOver    ReplicationStatus = 4
	ReplicationError          ReplicationStatus = 5
)

// String implements fmt.Stringer.
func (rs ReplicationStatus) String() string {
	switch rs {
	case InitializingReplication:
		return "initializing replication"
	case Replicating:
		return "replicating"
	case ReplicationPaused:
		return "replication paused"
	case ReplicationPendingCutover:
		return "replication pending cutover"
	case ReplicationCuttingOver:
		return "replication cutting over"
	case ReplicationError:
		return "replication error"
	default:
		return fmt.Sprintf("unimplemented-%d", int(rs))
	}
}

// AutoStatsName is the name to use for statistics created automatically.
// The name is chosen to be something that users are unlikely to choose when
// running CREATE STATISTICS manually.
const AutoStatsName = "__auto__"

// ImportStatsName is the name to use for statistics created automatically
// during import.
const ImportStatsName = "__import__"

// ForecastStatsName is the name to use for statistic forecasts.
const ForecastStatsName = "__forecast__"

// MergedStatsName is the name to use for a statistic that is
// a merged combination between a partial statistic and a full
// table statistic.
const MergedStatsName = "__merged__"

// AutomaticJobTypes is a list of automatic job types that currently exist.
var AutomaticJobTypes = [...]Type{
	TypeAutoCreateStats,
	TypeAutoSpanConfigReconciliation,
	TypeAutoSQLStatsCompaction,
	TypeAutoSchemaTelemetry,
}

// DetailsType returns the type for a payload detail.
func DetailsType(d isPayload_Details) (Type, error) {
	switch d := d.(type) {
	case *Payload_Backup:
		return TypeBackup, nil
	case *Payload_Restore:
		return TypeRestore, nil
	case *Payload_SchemaChange:
		return TypeSchemaChange, nil
	case *Payload_Import:
		return TypeImport, nil
	case *Payload_Changefeed:
		return TypeChangefeed, nil
	case *Payload_CreateStats:
		createStatsName := d.CreateStats.Name
		if createStatsName == AutoStatsName {
			return TypeAutoCreateStats, nil
		}
		return TypeCreateStats, nil
	case *Payload_SchemaChangeGC:
		return TypeSchemaChangeGC, nil
	case *Payload_TypeSchemaChange:
		return TypeTypeSchemaChange, nil
	case *Payload_StreamIngestion:
		return TypeStreamIngestion, nil
	case *Payload_NewSchemaChange:
		return TypeNewSchemaChange, nil
	case *Payload_Migration:
		return TypeMigration, nil
	case *Payload_AutoSpanConfigReconciliation:
		return TypeAutoSpanConfigReconciliation, nil
	case *Payload_AutoSQLStatsCompaction:
		return TypeAutoSQLStatsCompaction, nil
	case *Payload_StreamReplication:
		return TypeStreamReplication, nil
	case *Payload_RowLevelTTL:
		return TypeRowLevelTTL, nil
	case *Payload_SchemaTelemetry:
		return TypeAutoSchemaTelemetry, nil
	case *Payload_KeyVisualizerDetails:
		return TypeKeyVisualizer, nil
	default:
		return TypeUnspecified, errors.Newf("Payload.Type called on a payload with an unknown details type: %T", d)
	}
}

// ForEachType executes f for each job Type.
func ForEachType(f func(typ Type), includeTypeUnspecified bool) {
	start := TypeBackup
	if includeTypeUnspecified {
		start = TypeUnspecified
	}
	for typ := start; typ < NumJobTypes; typ++ {
		f(typ)
	}
}

// JobDetailsForEveryJobType is an array of Details keyed by every job type,
// except for jobspb.TypeUnspecified.
var JobDetailsForEveryJobType = map[Type]Details{
	TypeBackup:       BackupDetails{},
	TypeRestore:      RestoreDetails{},
	TypeSchemaChange: SchemaChangeDetails{},
	TypeImport:       ImportDetails{},
	TypeChangefeed:   ChangefeedDetails{},
	TypeCreateStats:  CreateStatsDetails{},
	TypeAutoCreateStats: CreateStatsDetails{
		Name: AutoStatsName,
	},
	TypeSchemaChangeGC:               SchemaChangeGCDetails{},
	TypeTypeSchemaChange:             TypeSchemaChangeDetails{},
	TypeStreamIngestion:              StreamIngestionDetails{},
	TypeNewSchemaChange:              NewSchemaChangeDetails{},
	TypeMigration:                    MigrationDetails{},
	TypeAutoSpanConfigReconciliation: AutoSpanConfigReconciliationDetails{},
	TypeAutoSQLStatsCompaction:       AutoSQLStatsCompactionDetails{},
	TypeStreamReplication:            StreamReplicationDetails{},
	TypeRowLevelTTL:                  RowLevelTTLDetails{},
	TypeAutoSchemaTelemetry:          SchemaTelemetryDetails{},
	TypeKeyVisualizer:                KeyVisualizerDetails{},
}

// WrapProgressDetails wraps a ProgressDetails object in the protobuf wrapper
// struct necessary to make it usable as the Details field of a Progress.
//
// Providing an unknown details type indicates programmer error and so causes a
// panic.
func WrapProgressDetails(details ProgressDetails) interface {
	isProgress_Details
} {
	switch d := details.(type) {
	case BackupProgress:
		return &Progress_Backup{Backup: &d}
	case RestoreProgress:
		return &Progress_Restore{Restore: &d}
	case SchemaChangeProgress:
		return &Progress_SchemaChange{SchemaChange: &d}
	case ImportProgress:
		return &Progress_Import{Import: &d}
	case ChangefeedProgress:
		return &Progress_Changefeed{Changefeed: &d}
	case CreateStatsProgress:
		return &Progress_CreateStats{CreateStats: &d}
	case SchemaChangeGCProgress:
		return &Progress_SchemaChangeGC{SchemaChangeGC: &d}
	case TypeSchemaChangeProgress:
		return &Progress_TypeSchemaChange{TypeSchemaChange: &d}
	case StreamIngestionProgress:
		return &Progress_StreamIngest{StreamIngest: &d}
	case NewSchemaChangeProgress:
		return &Progress_NewSchemaChange{NewSchemaChange: &d}
	case MigrationProgress:
		return &Progress_Migration{Migration: &d}
	case AutoSpanConfigReconciliationProgress:
		return &Progress_AutoSpanConfigReconciliation{AutoSpanConfigReconciliation: &d}
	case AutoSQLStatsCompactionProgress:
		return &Progress_AutoSQLStatsCompaction{AutoSQLStatsCompaction: &d}
	case StreamReplicationProgress:
		return &Progress_StreamReplication{StreamReplication: &d}
	case RowLevelTTLProgress:
		return &Progress_RowLevelTTL{RowLevelTTL: &d}
	case SchemaTelemetryProgress:
		return &Progress_SchemaTelemetry{SchemaTelemetry: &d}
	case KeyVisualizerProgress:
		return &Progress_KeyVisualizerProgress{KeyVisualizerProgress: &d}
	default:
		panic(errors.AssertionFailedf("WrapProgressDetails: unknown progress type %T", d))
	}
}

// UnwrapDetails returns the details object stored within the payload's Details
// field, discarding the protobuf wrapper struct.
func (p *Payload) UnwrapDetails() Details {
	switch d := p.Details.(type) {
	case *Payload_Backup:
		return *d.Backup
	case *Payload_Restore:
		return *d.Restore
	case *Payload_SchemaChange:
		return *d.SchemaChange
	case *Payload_Import:
		return *d.Import
	case *Payload_Changefeed:
		return *d.Changefeed
	case *Payload_CreateStats:
		return *d.CreateStats
	case *Payload_SchemaChangeGC:
		return *d.SchemaChangeGC
	case *Payload_TypeSchemaChange:
		return *d.TypeSchemaChange
	case *Payload_StreamIngestion:
		return *d.StreamIngestion
	case *Payload_NewSchemaChange:
		return *d.NewSchemaChange
	case *Payload_Migration:
		return *d.Migration
	case *Payload_AutoSpanConfigReconciliation:
		return *d.AutoSpanConfigReconciliation
	case *Payload_AutoSQLStatsCompaction:
		return *d.AutoSQLStatsCompaction
	case *Payload_StreamReplication:
		return *d.StreamReplication
	case *Payload_RowLevelTTL:
		return *d.RowLevelTTL
	case *Payload_SchemaTelemetry:
		return *d.SchemaTelemetry
	case *Payload_KeyVisualizerDetails:
		return *d.KeyVisualizerDetails
	default:
		return nil
	}
}

// UnwrapDetails returns the details object stored within the progress' Details
// field, discarding the protobuf wrapper struct.
func (p *Progress) UnwrapDetails() ProgressDetails {
	switch d := p.Details.(type) {
	case *Progress_Backup:
		return *d.Backup
	case *Progress_Restore:
		return *d.Restore
	case *Progress_SchemaChange:
		return *d.SchemaChange
	case *Progress_Import:
		return *d.Import
	case *Progress_Changefeed:
		return *d.Changefeed
	case *Progress_CreateStats:
		return *d.CreateStats
	case *Progress_SchemaChangeGC:
		return *d.SchemaChangeGC
	case *Progress_TypeSchemaChange:
		return *d.TypeSchemaChange
	case *Progress_StreamIngest:
		return *d.StreamIngest
	case *Progress_NewSchemaChange:
		return *d.NewSchemaChange
	case *Progress_Migration:
		return *d.Migration
	case *Progress_AutoSpanConfigReconciliation:
		return *d.AutoSpanConfigReconciliation
	case *Progress_AutoSQLStatsCompaction:
		return *d.AutoSQLStatsCompaction
	case *Progress_StreamReplication:
		return *d.StreamReplication
	case *Progress_RowLevelTTL:
		return *d.RowLevelTTL
	case *Progress_SchemaTelemetry:
		return *d.SchemaTelemetry
	case *Progress_KeyVisualizerProgress:
		return *d.KeyVisualizerProgress
	default:
		return nil
	}
}

func (t Type) String() string {
	// Protobufs, by convention, use CAPITAL_SNAKE_CASE for enum identifiers.
	// Since Type's string representation is used as a SHOW JOBS output column, we
	// simply swap underscores for spaces in the identifier for very SQL-esque
	// names, like "BACKUP" and "SCHEMA CHANGE".
	return strings.Replace(Type_name[int32(t)], "_", " ", -1)
}

// WrapPayloadDetails wraps a Details object in the protobuf wrapper struct
// necessary to make it usable as the Details field of a Payload.
//
// Providing an unknown details type indicates programmer error and so causes a
// panic.
func WrapPayloadDetails(details Details) interface {
	isPayload_Details
} {
	switch d := details.(type) {
	case BackupDetails:
		return &Payload_Backup{Backup: &d}
	case RestoreDetails:
		return &Payload_Restore{Restore: &d}
	case SchemaChangeDetails:
		return &Payload_SchemaChange{SchemaChange: &d}
	case ImportDetails:
		return &Payload_Import{Import: &d}
	case ChangefeedDetails:
		return &Payload_Changefeed{Changefeed: &d}
	case CreateStatsDetails:
		return &Payload_CreateStats{CreateStats: &d}
	case SchemaChangeGCDetails:
		return &Payload_SchemaChangeGC{SchemaChangeGC: &d}
	case TypeSchemaChangeDetails:
		return &Payload_TypeSchemaChange{TypeSchemaChange: &d}
	case StreamIngestionDetails:
		return &Payload_StreamIngestion{StreamIngestion: &d}
	case NewSchemaChangeDetails:
		return &Payload_NewSchemaChange{NewSchemaChange: &d}
	case MigrationDetails:
		return &Payload_Migration{Migration: &d}
	case AutoSpanConfigReconciliationDetails:
		return &Payload_AutoSpanConfigReconciliation{AutoSpanConfigReconciliation: &d}
	case AutoSQLStatsCompactionDetails:
		return &Payload_AutoSQLStatsCompaction{AutoSQLStatsCompaction: &d}
	case StreamReplicationDetails:
		return &Payload_StreamReplication{StreamReplication: &d}
	case RowLevelTTLDetails:
		return &Payload_RowLevelTTL{RowLevelTTL: &d}
	case SchemaTelemetryDetails:
		return &Payload_SchemaTelemetry{SchemaTelemetry: &d}
	case KeyVisualizerDetails:
		return &Payload_KeyVisualizerDetails{KeyVisualizerDetails: &d}
	default:
		panic(errors.AssertionFailedf("jobs.WrapPayloadDetails: unknown details type %T", d))
	}
}

// ChangefeedTargets is a set of id targets with metadata.
type ChangefeedTargets map[descpb.ID]ChangefeedTargetTable

// SchemaChangeDetailsFormatVersion is the format version for
// SchemaChangeDetails.
type SchemaChangeDetailsFormatVersion uint32

const (
	// BaseFormatVersion corresponds to the initial version of
	// SchemaChangeDetails, intended for the original version of schema change
	// jobs which were meant to be updated by a SchemaChanger instead of being run
	// as jobs by the job registry.
	BaseFormatVersion SchemaChangeDetailsFormatVersion = iota
	// JobResumerFormatVersion corresponds to the introduction of the schema
	// change job resumer. This version introduces the TableID and MutationID
	// fields, and, more generally, flags the job as being suitable for the job
	// registry to adopt.
	JobResumerFormatVersion
	// DatabaseJobFormatVersion indicates that database schema changes are
	// run in the schema change job.
	DatabaseJobFormatVersion

	// Silence unused warning.
	_ = BaseFormatVersion
)

// SafeValue implements the redact.SafeValue interface.
func (Type) SafeValue() {}

// NumJobTypes is the number of jobs types.
const NumJobTypes = 19

// ChangefeedDetailsMarshaler allows for dependency injection of
// cloud.SanitizeExternalStorageURI to avoid the dependency from this
// package on cloud. The value is injected in the changefeedccl package.
var ChangefeedDetailsMarshaler func(*ChangefeedDetails, *jsonpb.Marshaler) ([]byte, error)

// MarshalJSONPB implements jsonpb.JSONPBMarshaller to redact sensitive sink URI
// parameters from ChangefeedDetails.
func (m *ChangefeedDetails) MarshalJSONPB(marshaller *jsonpb.Marshaler) ([]byte, error) {
	if ChangefeedDetailsMarshaler != nil {
		return ChangefeedDetailsMarshaler(m, marshaller)
	}
	// if we get here, there's no injected marshaller, i.e. this could be an oss
	// binary looking at a ccl-made job, but we need to redact, so just redact
	// the entire struct by rendering an empty one.
	return json.Marshal(ChangefeedDetails{})
}

// DescRewriteMap maps old descriptor IDs to new descriptor and parent IDs.
type DescRewriteMap map[descpb.ID]*DescriptorRewrite

// assertDetailsMap asserts that the entries in JobDetailsForEveryJobType are correct.
func assertDetailsMap() {
	if len(JobDetailsForEveryJobType) != NumJobTypes-1 {
		panic("JobDetailsForEveryJobType does not have an entry for each Type")
	}
	ForEachType(
		func(typ Type) {
			payload := Payload{
				Details: WrapPayloadDetails(JobDetailsForEveryJobType[typ]),
			}
			if typ != payload.Type() {
				panic(fmt.Errorf("JobDetailsForEveryJobType has the incorrect entry for type %s", typ))
			}
		}, false,
	)
}

func init() {
	if len(Type_name) != NumJobTypes {
		panic(fmt.Errorf("NumJobTypes (%d) does not match generated job type name map length (%d)",
			NumJobTypes, len(Type_name)))
	}

	assertDetailsMap()

	protoreflect.RegisterShorthands((*Progress)(nil), "progress")
	protoreflect.RegisterShorthands((*Payload)(nil), "payload")
	protoreflect.RegisterShorthands((*ScheduleDetails)(nil), "schedule", "schedule_details")
	protoreflect.RegisterShorthands((*ExecutionArguments)(nil), "exec_args", "execution_args", "schedule_args")
}
