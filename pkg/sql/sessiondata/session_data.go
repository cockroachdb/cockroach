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

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
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
	sessiondatapb.LocalOnlySessionData
	// LocalUnmigratableSessionData contains session parameters that cannot
	// be propagated to remote nodes and cannot be migrated to another
	// session.
	LocalUnmigratableSessionData

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

// GetIntervalStyle returns the session interval style.
func (s *SessionData) GetIntervalStyle() duration.IntervalStyle {
	if s == nil {
		return duration.IntervalStyle_POSTGRES
	}
	return s.DataConversionConfig.IntervalStyle
}

// GetDateStyle returns the session date style.
func (s *SessionData) GetDateStyle() pgdate.DateStyle {
	if s == nil {
		return pgdate.DefaultDateStyle()
	}
	return s.DataConversionConfig.DateStyle
}

// LocalUnmigratableSessionData contains session parameters that cannot
// be propagated to remote nodes and cannot be migrated to another
// session.
type LocalUnmigratableSessionData struct {
	// RemoteAddr is used to generate logging events.
	// RemoteAddr will acceptably change between session migrations.
	RemoteAddr net.Addr
	// DatabaseIDToTempSchemaID stores the temp schema ID for every
	// database that has created a temporary schema. The mapping is from
	// descpb.ID -> descpb.ID, but cannot be stored as such due to package
	// dependencies. Temporary tables are not supported in session migrations.
	DatabaseIDToTempSchemaID map[uint32]uint32
	// ExperimentalDistSQLPlanningMode indicates whether the experimental
	// DistSQL planning driven by the optimizer is enabled.
	ExperimentalDistSQLPlanningMode ExperimentalDistSQLPlanningMode
	// DistSQLMode indicates whether to run queries using the distributed
	// execution engine.
	DistSQLMode DistSQLExecMode
	// SerialNormalizationMode indicates how to handle the SERIAL pseudo-type.
	SerialNormalizationMode SerialNormalizationMode
	// NewSchemaChangerMode indicates whether to use the new schema changer.
	NewSchemaChangerMode NewSchemaChangerMode
	// SequenceCache stores sequence values which have been cached using the
	// CACHE sequence option.
	// Cached sequence options are not yet supported during session migrations.
	SequenceCache SequenceCache

	///////////////////////////////////////////////////////////////////////////
	// WARNING: consider whether a session parameter you're adding needs to  //
	// be propagated to the remote nodes or needs to persist amongst session //
	// migrations. If so, they should live in the LocalOnlySessionData or    //
	// SessionData protobuf in the sessiondatapb package.                    //
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
