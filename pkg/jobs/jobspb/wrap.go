// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

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
	_ Details = AutoConfigRunnerDetails{}
	_ Details = AutoConfigEnvRunnerDetails{}
	_ Details = AutoConfigTaskDetails{}
	_ Details = AutoUpdateSQLActivityDetails{}
	_ Details = MVCCStatisticsJobDetails{}
	_ Details = ImportRollbackDetails{}
	_ Details = HistoryRetentionDetails{}
	_ Details = LogicalReplicationDetails{}
	_ Details = UpdateTableMetadataCacheDetails{}
	_ Details = StandbyReadTSPollerDetails{}
	_ Details = SqlActivityFlushDetails{}
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
	_ ProgressDetails = AutoConfigRunnerProgress{}
	_ ProgressDetails = AutoConfigEnvRunnerProgress{}
	_ ProgressDetails = AutoConfigTaskProgress{}
	_ ProgressDetails = AutoUpdateSQLActivityProgress{}
	_ ProgressDetails = MVCCStatisticsJobProgress{}
	_ ProgressDetails = ImportRollbackProgress{}
	_ ProgressDetails = HistoryRetentionProgress{}
	_ ProgressDetails = LogicalReplicationProgress{}
	_ ProgressDetails = UpdateTableMetadataCacheProgress{}
	_ ProgressDetails = StandbyReadTSPollerProgress{}
	_ ProgressDetails = SqlActivityFlushProgress{}
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
	InitializingReplication    ReplicationStatus = 0
	CreatingInitialSplits      ReplicationStatus = 6
	Replicating                ReplicationStatus = 1
	ReplicationPaused          ReplicationStatus = 2
	ReplicationPendingFailover ReplicationStatus = 3
	ReplicationFailingOver     ReplicationStatus = 4
	ReplicationError           ReplicationStatus = 5
	InitialScan                ReplicationStatus = 7
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
	case ReplicationPendingFailover:
		return "replication pending failover"
	case ReplicationFailingOver:
		return "replication failing over"
	case ReplicationError:
		return "replication error"
	case CreatingInitialSplits:
		return "creating initial splits"
	case InitialScan:
		return "running initial scan"
	default:
		return fmt.Sprintf("unimplemented-%d", int(rs))
	}
}

// AutoStatsName is the name to use for statistics created automatically.
// The name is chosen to be something that users are unlikely to choose when
// running CREATE STATISTICS manually.
const AutoStatsName = "__auto__"

// AutoPartialStatsName is the name to use for partial statistics created
// automatically.
const AutoPartialStatsName = "__auto_partial__"

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
	TypeAutoCreatePartialStats,
	TypeAutoSpanConfigReconciliation,
	TypeAutoSQLStatsCompaction,
	TypeAutoSchemaTelemetry,
	TypePollJobsStats,
	TypeAutoConfigRunner,
	TypeAutoConfigEnvRunner,
	TypeAutoConfigTask,
	TypeKeyVisualizer,
	TypeAutoUpdateSQLActivity,
	TypeMVCCStatisticsUpdate,
	TypeUpdateTableMetadataCache,
	TypeSQLActivityFlush,
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
		} else if createStatsName == AutoPartialStatsName {
			return TypeAutoCreatePartialStats, nil
		}
		return TypeCreateStats, nil
	case *Payload_SchemaChangeGC:
		return TypeSchemaChangeGC, nil
	case *Payload_TypeSchemaChange:
		return TypeTypeSchemaChange, nil
	case *Payload_StreamIngestion:
		return TypeReplicationStreamIngestion, nil
	case *Payload_NewSchemaChange:
		return TypeNewSchemaChange, nil
	case *Payload_Migration:
		return TypeMigration, nil
	case *Payload_AutoSpanConfigReconciliation:
		return TypeAutoSpanConfigReconciliation, nil
	case *Payload_AutoSQLStatsCompaction:
		return TypeAutoSQLStatsCompaction, nil
	case *Payload_StreamReplication:
		return TypeReplicationStreamProducer, nil
	case *Payload_RowLevelTTL:
		return TypeRowLevelTTL, nil
	case *Payload_SchemaTelemetry:
		return TypeAutoSchemaTelemetry, nil
	case *Payload_KeyVisualizerDetails:
		return TypeKeyVisualizer, nil
	case *Payload_PollJobsStats:
		return TypePollJobsStats, nil
	case *Payload_AutoConfigRunner:
		return TypeAutoConfigRunner, nil
	case *Payload_AutoConfigEnvRunner:
		return TypeAutoConfigEnvRunner, nil
	case *Payload_AutoConfigTask:
		return TypeAutoConfigTask, nil
	case *Payload_AutoUpdateSqlActivities:
		return TypeAutoUpdateSQLActivity, nil
	case *Payload_MvccStatisticsDetails:
		return TypeMVCCStatisticsUpdate, nil
	case *Payload_ImportRollbackDetails:
		return TypeImportRollback, nil
	case *Payload_HistoryRetentionDetails:
		return TypeHistoryRetention, nil
	case *Payload_LogicalReplicationDetails:
		return TypeLogicalReplication, nil
	case *Payload_UpdateTableMetadataCacheDetails:
		return TypeUpdateTableMetadataCache, nil
	case *Payload_StandbyReadTsPollerDetails:
		return TypeStandbyReadTSPoller, nil
	case *Payload_SqlActivityFlushDetails:
		return TypeSQLActivityFlush, nil
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
	TypeAutoCreatePartialStats: CreateStatsDetails{
		Name: AutoPartialStatsName,
	},
	TypeSchemaChangeGC:               SchemaChangeGCDetails{},
	TypeTypeSchemaChange:             TypeSchemaChangeDetails{},
	TypeReplicationStreamIngestion:   StreamIngestionDetails{},
	TypeNewSchemaChange:              NewSchemaChangeDetails{},
	TypeMigration:                    MigrationDetails{},
	TypeAutoSpanConfigReconciliation: AutoSpanConfigReconciliationDetails{},
	TypeAutoSQLStatsCompaction:       AutoSQLStatsCompactionDetails{},
	TypeReplicationStreamProducer:    StreamReplicationDetails{},
	TypeRowLevelTTL:                  RowLevelTTLDetails{},
	TypeAutoSchemaTelemetry:          SchemaTelemetryDetails{},
	TypeKeyVisualizer:                KeyVisualizerDetails{},
	TypePollJobsStats:                PollJobsStatsDetails{},
	TypeAutoConfigRunner:             AutoConfigRunnerDetails{},
	TypeAutoConfigEnvRunner:          AutoConfigEnvRunnerDetails{},
	TypeAutoConfigTask:               AutoConfigTaskDetails{},
	TypeAutoUpdateSQLActivity:        AutoUpdateSQLActivityDetails{},
	TypeMVCCStatisticsUpdate:         MVCCStatisticsJobDetails{},
	TypeImportRollback:               ImportRollbackDetails{},
	TypeHistoryRetention:             HistoryRetentionDetails{},
	TypeLogicalReplication:           LogicalReplicationDetails{},
	TypeUpdateTableMetadataCache:     UpdateTableMetadataCacheDetails{},
	TypeStandbyReadTSPoller:          StandbyReadTSPollerDetails{},
	TypeSQLActivityFlush:             SqlActivityFlushDetails{},
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
	case PollJobsStatsProgress:
		return &Progress_PollJobsStats{PollJobsStats: &d}
	case AutoConfigRunnerProgress:
		return &Progress_AutoConfigRunner{AutoConfigRunner: &d}
	case AutoConfigEnvRunnerProgress:
		return &Progress_AutoConfigEnvRunner{AutoConfigEnvRunner: &d}
	case AutoConfigTaskProgress:
		return &Progress_AutoConfigTask{AutoConfigTask: &d}
	case AutoUpdateSQLActivityProgress:
		return &Progress_UpdateSqlActivity{UpdateSqlActivity: &d}
	case MVCCStatisticsJobProgress:
		return &Progress_MvccStatisticsProgress{MvccStatisticsProgress: &d}
	case ImportRollbackProgress:
		return &Progress_ImportRollbackProgress{ImportRollbackProgress: &d}
	case HistoryRetentionProgress:
		return &Progress_HistoryRetentionProgress{HistoryRetentionProgress: &d}
	case LogicalReplicationProgress:
		return &Progress_LogicalReplication{LogicalReplication: &d}
	case UpdateTableMetadataCacheProgress:
		return &Progress_TableMetadataCache{TableMetadataCache: &d}
	case StandbyReadTSPollerProgress:
		return &Progress_StandbyReadTsPoller{StandbyReadTsPoller: &d}
	case SqlActivityFlushProgress:
		return &Progress_SqlActivityFlush{SqlActivityFlush: &d}
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
	case *Payload_PollJobsStats:
		return *d.PollJobsStats
	case *Payload_AutoConfigRunner:
		return *d.AutoConfigRunner
	case *Payload_AutoConfigEnvRunner:
		return *d.AutoConfigEnvRunner
	case *Payload_AutoConfigTask:
		return *d.AutoConfigTask
	case *Payload_AutoUpdateSqlActivities:
		return *d.AutoUpdateSqlActivities
	case *Payload_MvccStatisticsDetails:
		return *d.MvccStatisticsDetails
	case *Payload_ImportRollbackDetails:
		return *d.ImportRollbackDetails
	case *Payload_HistoryRetentionDetails:
		return *d.HistoryRetentionDetails
	case *Payload_LogicalReplicationDetails:
		return *d.LogicalReplicationDetails
	case *Payload_UpdateTableMetadataCacheDetails:
		return *d.UpdateTableMetadataCacheDetails
	case *Payload_StandbyReadTsPollerDetails:
		return *d.StandbyReadTsPollerDetails
	case *Payload_SqlActivityFlushDetails:
		return *d.SqlActivityFlushDetails
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
	case *Progress_PollJobsStats:
		return *d.PollJobsStats
	case *Progress_AutoConfigRunner:
		return *d.AutoConfigRunner
	case *Progress_AutoConfigEnvRunner:
		return *d.AutoConfigEnvRunner
	case *Progress_AutoConfigTask:
		return *d.AutoConfigTask
	case *Progress_UpdateSqlActivity:
		return *d.UpdateSqlActivity
	case *Progress_MvccStatisticsProgress:
		return *d.MvccStatisticsProgress
	case *Progress_ImportRollbackProgress:
		return *d.ImportRollbackProgress
	case *Progress_HistoryRetentionProgress:
		return *d.HistoryRetentionProgress
	case *Progress_LogicalReplication:
		return *d.LogicalReplication
	case *Progress_TableMetadataCache:
		return *d.TableMetadataCache
	case *Progress_StandbyReadTsPoller:
		return *d.StandbyReadTsPoller
	case *Progress_SqlActivityFlush:
		return *d.SqlActivityFlush
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

// TypeFromString is used to get the type corresponding to the string s
// where s := Type.String().
func TypeFromString(s string) (Type, error) {
	s = strings.Replace(s, " ", "_", -1)
	t, ok := Type_value[s]
	if !ok {
		return TypeUnspecified, errors.New("invalid type string")
	}
	return Type(t), nil
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
	case PollJobsStatsDetails:
		return &Payload_PollJobsStats{PollJobsStats: &d}
	case AutoConfigRunnerDetails:
		return &Payload_AutoConfigRunner{AutoConfigRunner: &d}
	case AutoConfigEnvRunnerDetails:
		return &Payload_AutoConfigEnvRunner{AutoConfigEnvRunner: &d}
	case AutoConfigTaskDetails:
		return &Payload_AutoConfigTask{AutoConfigTask: &d}
	case AutoUpdateSQLActivityDetails:
		return &Payload_AutoUpdateSqlActivities{AutoUpdateSqlActivities: &d}
	case MVCCStatisticsJobDetails:
		return &Payload_MvccStatisticsDetails{MvccStatisticsDetails: &d}
	case ImportRollbackDetails:
		return &Payload_ImportRollbackDetails{ImportRollbackDetails: &d}
	case HistoryRetentionDetails:
		return &Payload_HistoryRetentionDetails{HistoryRetentionDetails: &d}
	case LogicalReplicationDetails:
		return &Payload_LogicalReplicationDetails{LogicalReplicationDetails: &d}
	case UpdateTableMetadataCacheDetails:
		return &Payload_UpdateTableMetadataCacheDetails{UpdateTableMetadataCacheDetails: &d}
	case StandbyReadTSPollerDetails:
		return &Payload_StandbyReadTsPollerDetails{StandbyReadTsPollerDetails: &d}
	case SqlActivityFlushDetails:
		return &Payload_SqlActivityFlushDetails{SqlActivityFlushDetails: &d}
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
const NumJobTypes = 32

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
