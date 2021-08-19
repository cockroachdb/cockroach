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
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
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

// SessionUser retrieves the session_user.
// This currently returns current_user, as session_user is not implemented.
func (s *SessionData) SessionUser() security.SQLUsername {
	return s.User()
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
