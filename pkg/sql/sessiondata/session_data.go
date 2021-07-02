// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondata

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SessionData contains session parameters. They are all user-configurable.
// A SQL Session changes fields in SessionData through sql.sessionDataMutator.
type SessionData struct {
	// SessionData contains session parameters that are easily serializable and
	// are required to be propagated to the remote nodes for the correct
	// execution of DistSQL flows.
	sessiondatapb.SessionData
	// LocalOnlySessionData contains session parameters that don't need to be
	// propagated to the remote nodes.
	LocalOnlySessionData

	// All session parameters below must be propagated to the remote nodes but
	// are not easily serializable. They require custom serialization
	// (MarshalNonLocal) and deserialization (UnmarshalNonLocal).
	//
	// Location indicates the current time zone.
	Location *time.Location
	// SearchPath is a list of namespaces to search builtins in.
	SearchPath SearchPath
	// SequenceState gives access to the SQL sequences that have been
	// manipulated by the session.
	SequenceState *SequenceState
}

// MarshalNonLocal serializes all non-local parameters from SessionData struct
// that don't have native protobuf support into proto.
func MarshalNonLocal(sd *SessionData, proto *sessiondatapb.SessionData) {
	proto.Location = sd.GetLocation().String()
	// Populate the search path. Make sure not to include the implicit pg_catalog,
	// since the remote end already knows to add the implicit pg_catalog if
	// necessary, and sending it over would make the remote end think that
	// pg_catalog was explicitly included by the user.
	proto.SearchPath = sd.SearchPath.GetPathArray()
	proto.TemporarySchemaName = sd.SearchPath.GetTemporarySchemaName()
	// Populate the sequences state.
	latestValues, lastIncremented := sd.SequenceState.Export()
	if len(latestValues) > 0 {
		proto.SeqState.LastSeqIncremented = lastIncremented
		for seqID, latestVal := range latestValues {
			proto.SeqState.Seqs = append(proto.SeqState.Seqs,
				&sessiondatapb.SequenceState_Seq{SeqID: seqID, LatestVal: latestVal},
			)
		}
	}
}

// UnmarshalNonLocal returns a new SessionData based on the serialized
// representation. Note that only non-local session parameters are populated.
func UnmarshalNonLocal(proto sessiondatapb.SessionData) (*SessionData, error) {
	location, err := timeutil.TimeZoneStringToLocation(
		proto.Location,
		timeutil.TimeZoneStringToLocationISO8601Standard,
	)
	if err != nil {
		return nil, err
	}
	seqState := NewSequenceState()
	var haveSequences bool
	for _, seq := range proto.SeqState.Seqs {
		seqState.RecordValue(seq.SeqID, seq.LatestVal)
		haveSequences = true
	}
	if haveSequences {
		seqState.SetLastSequenceIncremented(proto.SeqState.LastSeqIncremented)
	}
	return &SessionData{
		SessionData: proto,
		SearchPath: MakeSearchPath(
			proto.SearchPath,
		).WithTemporarySchemaName(
			proto.TemporarySchemaName,
		).WithUserSchemaName(proto.UserProto.Decode().Normalized()),
		SequenceState: seqState,
		Location:      location,
	}, nil
}

// GetLocation returns the session timezone.
func (s *SessionData) GetLocation() *time.Location {
	if s == nil || s.Location == nil {
		return time.UTC
	}
	return s.Location
}

// LocalOnlySessionData contains session parameters that only influence the
// execution on the gateway node and don't need to be propagated to the remote
// nodes.
type LocalOnlySessionData struct {
	// SaveTablesPrefix indicates that a table should be created with the
	// given prefix for the output of each subexpression in a query. If
	// SaveTablesPrefix is empty, no tables are created.
	SaveTablesPrefix string
	// RemoteAddr is used to generate logging events.
	RemoteAddr net.Addr
	// ExperimentalDistSQLPlanningMode indicates whether the experimental
	// DistSQL planning driven by the optimizer is enabled.
	ExperimentalDistSQLPlanningMode ExperimentalDistSQLPlanningMode
	// DistSQLMode indicates whether to run queries using the distributed
	// execution engine.
	DistSQLMode DistSQLExecMode
	// OptimizerFKCascadesLimit is the maximum number of cascading operations that
	// are run for a single query.
	OptimizerFKCascadesLimit int
	// ResultsBufferSize specifies the size at which the pgwire results buffer
	// will self-flush.
	ResultsBufferSize int64
	// NoticeDisplaySeverity indicates the level of Severity to send notices for the given
	// session.
	NoticeDisplaySeverity pgnotice.DisplaySeverity
	// SerialNormalizationMode indicates how to handle the SERIAL pseudo-type.
	SerialNormalizationMode SerialNormalizationMode
	// DatabaseIDToTempSchemaID stores the temp schema ID for every database that
	// has created a temporary schema. The mapping is from descpb.ID -> desscpb.ID,
	// but cannot be stored as such due to package dependencies.
	DatabaseIDToTempSchemaID map[uint32]uint32
	// StmtTimeout is the duration a query is permitted to run before it is
	// canceled by the session. If set to 0, there is no timeout.
	StmtTimeout time.Duration
	// IdleInSessionTimeout is the duration a session is permitted to idle before
	// the session is canceled. If set to 0, there is no timeout.
	IdleInSessionTimeout time.Duration
	// IdleInTransactionSessionTimeout is the duration a session is permitted to
	// idle in a transaction before the session is canceled.
	// If set to 0, there is no timeout.
	IdleInTransactionSessionTimeout time.Duration
	// ReorderJoinsLimit indicates the number of joins at which the optimizer should
	// stop attempting to reorder.
	ReorderJoinsLimit int
	// DefaultTxnPriority indicates the default priority of newly created
	// transactions.
	// NOTE: we'd prefer to use tree.UserPriority here, but doing so would
	// introduce a package dependency cycle.
	DefaultTxnPriority int
	// DefaultTxnReadOnly indicates the default read-only status of newly
	// created transactions.
	DefaultTxnReadOnly bool
	// DefaultTxnUseFollowerReads indicates whether transactions should be
	// created by default using an AS OF SYSTEM TIME clause far enough in the
	// past to facilitate reads against followers. If true, transactions will
	// also default to being read-only.
	DefaultTxnUseFollowerReads bool
	// PartiallyDistributedPlansDisabled indicates whether the partially
	// distributed plans produced by distSQLSpecExecFactory are disabled. It
	// should be set to 'true' only in tests that verify that the old and the
	// new factories return exactly the same physical plans.
	// TODO(yuzefovich): remove it when deleting old sql.execFactory.
	PartiallyDistributedPlansDisabled bool
	// OptimizerUseHistograms indicates whether we should use histograms for
	// cardinality estimation in the optimizer.
	OptimizerUseHistograms bool
	// OptimizerUseMultiColStats indicates whether we should use multi-column
	// statistics for cardinality estimation in the optimizer.
	OptimizerUseMultiColStats bool
	// LocalityOptimizedSearch indicates that the optimizer will try to plan scans
	// and lookup joins in which local nodes (i.e., nodes in the gateway region)
	// are searched for matching rows before remote nodes, in the hope that the
	// execution engine can avoid visiting remote nodes.
	LocalityOptimizedSearch bool
	// SafeUpdates causes errors when the client
	// sends syntax that may have unwanted side effects.
	SafeUpdates bool
	// PreferLookupJoinsForFKs causes foreign key operations to prefer lookup
	// joins.
	PreferLookupJoinsForFKs bool
	// ZigzagJoinEnabled indicates whether the optimizer should try and plan a
	// zigzag join.
	ZigzagJoinEnabled bool
	// RequireExplicitPrimaryKeys indicates whether CREATE TABLE statements should
	// error out if no primary key is provided.
	RequireExplicitPrimaryKeys bool
	// ForceSavepointRestart overrides the default SAVEPOINT behavior
	// for compatibility with certain ORMs. When this flag is set,
	// the savepoint name will no longer be compared against the magic
	// identifier `cockroach_restart` in order use a restartable
	// transaction.
	ForceSavepointRestart bool
	// AllowPrepareAsOptPlan must be set to allow use of
	//   PREPARE name AS OPT PLAN '...'
	AllowPrepareAsOptPlan bool
	// TempTablesEnabled indicates whether temporary tables can be created or not.
	TempTablesEnabled bool
	// ImplicitPartitioningEnabled indicates whether implicit column partitioning
	// can be created.
	ImplicitColumnPartitioningEnabled bool
	// DropEnumValueEnabled indicates whether enum values can be dropped.
	DropEnumValueEnabled bool
	// OverrideMultiRegionZoneConfigEnabled indicates whether zone configurations can be
	// modified for multi-region databases and tables/indexes/partitions.
	OverrideMultiRegionZoneConfigEnabled bool
	// HashShardedIndexesEnabled indicates whether hash sharded indexes can be created.
	HashShardedIndexesEnabled bool
	// DisallowFullTableScans indicates whether queries that plan full table scans
	// should be rejected.
	DisallowFullTableScans bool
	// ImplicitSelectForUpdate is true if FOR UPDATE locking may be used during
	// the row-fetch phase of mutation statements.
	ImplicitSelectForUpdate bool
	// InsertFastPath is true if the fast path for insert (with VALUES input) may
	// be used.
	InsertFastPath bool
	// AlterColumnTypeGeneralEnabled is true if ALTER TABLE ... ALTER COLUMN ...
	// TYPE x may be used for general conversions requiring online schema change/
	AlterColumnTypeGeneralEnabled bool
	// SynchronousCommit is a dummy setting for the synchronous_commit var.
	SynchronousCommit bool
	// EnableSeqScan is a dummy setting for the enable_seqscan var.
	EnableSeqScan bool

	// EnableExpressionIndexes indicates whether creating expression indexes is
	// allowed.
	EnableExpressionIndexes bool

	// EnableUniqueWithoutIndexConstraints indicates whether creating unique
	// constraints without an index is allowed.
	// TODO(rytaft): remove this once unique without index constraints are fully
	// supported.
	EnableUniqueWithoutIndexConstraints bool

	// NewSchemaChangerMode indicates whether to use the new schema changer.
	NewSchemaChangerMode NewSchemaChangerMode

	// EnableStreamReplication indicates whether to allow setting up a replication
	// stream.
	EnableStreamReplication bool

	// SequenceCache stores sequence values which have been cached using the
	// CACHE sequence option.
	SequenceCache SequenceCache

	// StubCatalogTablesEnabled allows queries against virtual
	// tables that are not yet implemented.
	StubCatalogTablesEnabled bool

	// ExperimentalComputedColumnRewrites allows automatic rewriting of computed
	// column expressions in CREATE TABLE and ALTER TABLE statements. See the
	// experimentalComputedColumnRewrites cluster setting for a description of the
	// format.
	ExperimentalComputedColumnRewrites string

	///////////////////////////////////////////////////////////////////////////
	// WARNING: consider whether a session parameter you're adding needs to  //
	// be propagated to the remote nodes. If so, that parameter should live  //
	// in the SessionData struct above.                                      //
	///////////////////////////////////////////////////////////////////////////
}

// IsTemporarySchemaID returns true if the given ID refers to any of the temp
// schemas created by the session.
func (s *SessionData) IsTemporarySchemaID(schemaID uint32) bool {
	_, exists := s.MaybeGetDatabaseForTemporarySchemaID(schemaID)
	return exists
}

// MaybeGetDatabaseForTemporarySchemaID returns the corresponding database and
// true if the schemaID refers to any of the temp schemas created by this
// session.
func (s *SessionData) MaybeGetDatabaseForTemporarySchemaID(schemaID uint32) (uint32, bool) {
	for dbID, tempSchemaID := range s.DatabaseIDToTempSchemaID {
		if tempSchemaID == schemaID {
			return dbID, true
		}
	}
	return 0, false
}

// GetTemporarySchemaIDForDb returns the schemaID for the temporary schema if
// one exists for the DB. The second return value communicates the existence of
// the temp schema for that DB.
func (s *SessionData) GetTemporarySchemaIDForDb(dbID uint32) (uint32, bool) {
	schemaID, found := s.DatabaseIDToTempSchemaID[dbID]
	return schemaID, found
}

// ExperimentalDistSQLPlanningMode controls if and when the opt-driven DistSQL
// planning is used to create physical plans.
type ExperimentalDistSQLPlanningMode int64

const (
	// ExperimentalDistSQLPlanningOff means that we always use the old path of
	// going from opt.Expr to planNodes and then to processor specs.
	ExperimentalDistSQLPlanningOff ExperimentalDistSQLPlanningMode = iota
	// ExperimentalDistSQLPlanningOn means that we will attempt to use the new
	// path for performing DistSQL planning in the optimizer, and if that
	// doesn't succeed for some reason, we will fallback to the old path.
	ExperimentalDistSQLPlanningOn
	// ExperimentalDistSQLPlanningAlways means that we will only use the new path,
	// and if it fails for any reason, the query will fail as well.
	ExperimentalDistSQLPlanningAlways
)

func (m ExperimentalDistSQLPlanningMode) String() string {
	switch m {
	case ExperimentalDistSQLPlanningOff:
		return "off"
	case ExperimentalDistSQLPlanningOn:
		return "on"
	case ExperimentalDistSQLPlanningAlways:
		return "always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// ExperimentalDistSQLPlanningModeFromString converts a string into a
// ExperimentalDistSQLPlanningMode. False is returned if the conversion was
// unsuccessful.
func ExperimentalDistSQLPlanningModeFromString(val string) (ExperimentalDistSQLPlanningMode, bool) {
	var m ExperimentalDistSQLPlanningMode
	switch strings.ToUpper(val) {
	case "OFF":
		m = ExperimentalDistSQLPlanningOff
	case "ON":
		m = ExperimentalDistSQLPlanningOn
	case "ALWAYS":
		m = ExperimentalDistSQLPlanningAlways
	default:
		return 0, false
	}
	return m, true
}

// DistSQLExecMode controls if and when the Executor distributes queries.
// Since 2.1, we run everything through the DistSQL infrastructure,
// and these settings control whether to use a distributed plan, or use a plan
// that only involves local DistSQL processors.
type DistSQLExecMode int64

const (
	// DistSQLOff means that we never distribute queries.
	DistSQLOff DistSQLExecMode = iota
	// DistSQLAuto means that we automatically decide on a case-by-case basis if
	// we distribute queries.
	DistSQLAuto
	// DistSQLOn means that we distribute queries that are supported.
	DistSQLOn
	// DistSQLAlways means that we only distribute; unsupported queries fail.
	DistSQLAlways
)

func (m DistSQLExecMode) String() string {
	switch m {
	case DistSQLOff:
		return "off"
	case DistSQLAuto:
		return "auto"
	case DistSQLOn:
		return "on"
	case DistSQLAlways:
		return "always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// DistSQLExecModeFromString converts a string into a DistSQLExecMode
func DistSQLExecModeFromString(val string) (_ DistSQLExecMode, ok bool) {
	switch strings.ToUpper(val) {
	case "OFF":
		return DistSQLOff, true
	case "AUTO":
		return DistSQLAuto, true
	case "ON":
		return DistSQLOn, true
	case "ALWAYS":
		return DistSQLAlways, true
	default:
		return 0, false
	}
}

// SerialNormalizationMode controls if and when the Executor uses DistSQL.
type SerialNormalizationMode int64

const (
	// SerialUsesRowID means use INT NOT NULL DEFAULT unique_rowid().
	SerialUsesRowID SerialNormalizationMode = iota
	// SerialUsesVirtualSequences means create a virtual sequence and
	// use INT NOT NULL DEFAULT nextval(...).
	SerialUsesVirtualSequences
	// SerialUsesSQLSequences means create a regular SQL sequence and
	// use INT NOT NULL DEFAULT nextval(...). Each call to nextval()
	// is a distributed call to kv. This minimizes the size of gaps
	// between successive sequence numbers (which occur due to
	// node failures or errors), but the multiple kv calls
	// can impact performance negatively.
	SerialUsesSQLSequences
	// SerialUsesCachedSQLSequences is identical to SerialUsesSQLSequences with
	// the exception that nodes can cache sequence values. This significantly
	// reduces contention and distributed calls to kv, which results in better
	// performance. Gaps between sequences may be larger as a result of cached
	// values being lost to errors and/or node failures.
	SerialUsesCachedSQLSequences
)

func (m SerialNormalizationMode) String() string {
	switch m {
	case SerialUsesRowID:
		return "rowid"
	case SerialUsesVirtualSequences:
		return "virtual_sequence"
	case SerialUsesSQLSequences:
		return "sql_sequence"
	case SerialUsesCachedSQLSequences:
		return "sql_sequence_cached"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// SerialNormalizationModeFromString converts a string into a SerialNormalizationMode
func SerialNormalizationModeFromString(val string) (_ SerialNormalizationMode, ok bool) {
	switch strings.ToUpper(val) {
	case "ROWID":
		return SerialUsesRowID, true
	case "VIRTUAL_SEQUENCE":
		return SerialUsesVirtualSequences, true
	case "SQL_SEQUENCE":
		return SerialUsesSQLSequences, true
	case "SQL_SEQUENCE_CACHED":
		return SerialUsesCachedSQLSequences, true
	default:
		return 0, false
	}
}

// NewSchemaChangerMode controls if and when the new schema changer (in
// sql/schemachanger) is in use.
type NewSchemaChangerMode int64

const (
	// UseNewSchemaChangerOff means that we never use the new schema changer.
	UseNewSchemaChangerOff NewSchemaChangerMode = iota
	// UseNewSchemaChangerOn means that we use the new schema changer for
	// supported statements in implicit transactions, but fall back to the old
	// schema changer otherwise.
	UseNewSchemaChangerOn
	// UseNewSchemaChangerUnsafeAlways means that we attempt to use the new schema
	// changer for all statements and return errors for unsupported statements.
	// Used for testing/development.
	UseNewSchemaChangerUnsafeAlways
)

func (m NewSchemaChangerMode) String() string {
	switch m {
	case UseNewSchemaChangerOff:
		return "off"
	case UseNewSchemaChangerOn:
		return "on"
	case UseNewSchemaChangerUnsafeAlways:
		return "unsafe_always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// NewSchemaChangerModeFromString converts a string into a NewSchemaChangerMode
func NewSchemaChangerModeFromString(val string) (_ NewSchemaChangerMode, ok bool) {
	switch strings.ToUpper(val) {
	case "OFF":
		return UseNewSchemaChangerOff, true
	case "ON":
		return UseNewSchemaChangerOn, true
	case "UNSAFE_ALWAYS":
		return UseNewSchemaChangerUnsafeAlways, true
	default:
		return 0, false
	}
}
