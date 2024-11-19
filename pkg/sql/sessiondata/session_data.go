// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondata

import (
	"net"
	"reflect"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

// Clone returns a clone of SessionData.
func (s *SessionData) Clone() *SessionData {
	var newCustomOptions map[string]string
	if len(s.CustomOptions) > 0 {
		newCustomOptions = make(map[string]string, len(s.CustomOptions))
		for k, v := range s.CustomOptions {
			newCustomOptions[k] = v
		}
	}
	// Other options in SessionData are shallow cloned - we can get away with it
	// as all the slices/maps does a copy if it mutates OR are operations that
	// affect the whole SessionDataStack (e.g. setting SequenceState should be the
	// setting the same value across all copied SessionData).
	ret := *s
	ret.CustomOptions = newCustomOptions
	return &ret
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
// The SessionUser is the username that originally logged into the session.
// If a user applies SET ROLE, the SessionUser remains the same whilst the
// User() changes.
func (s *SessionData) SessionUser() username.SQLUsername {
	if s.SessionUserProto == "" {
		return s.User()
	}
	return s.SessionUserProto.Decode()
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

	// IsSSL indicates whether the session is using SSL/TLS.
	IsSSL bool

	// AuthenticationMethod is the method used to authenticate this session.
	AuthenticationMethod redact.SafeString

	// ////////////////////////////////////////////////////////////////////////
	// WARNING: consider whether a session parameter you're adding needs to  //
	// be propagated to the remote nodes or needs to persist amongst session //
	// migrations. If so, they should live in the LocalOnlySessionData or    //
	// SessionData protobuf in the sessiondatapb package.                    //
	// ////////////////////////////////////////////////////////////////////////
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

// GetTemporarySchemaIDForDB returns the schemaID for the temporary schema if
// one exists for the DB. The second return value communicates the existence of
// the temp schema for that DB.
func (s *SessionData) GetTemporarySchemaIDForDB(dbID uint32) (uint32, bool) {
	schemaID, found := s.DatabaseIDToTempSchemaID[dbID]
	return schemaID, found
}

// Stack represents a stack of SessionData objects.
// This is used to support transaction-scoped variables, where SET LOCAL only
// affects the top of the stack.
// There is always guaranteed to be one element in the stack.
type Stack struct {
	// Use an internal variable to prevent abstraction leakage.
	stack []*SessionData
	// base is a pointer to the first element of the stack.
	// This avoids a race with stack being reassigned, as the first element
	// is *always* set.
	base *SessionData
}

// NewStack creates a new tack.
func NewStack(firstElem *SessionData) *Stack {
	return &Stack{stack: []*SessionData{firstElem}, base: firstElem}
}

// Clone clones the current stack.
func (s *Stack) Clone() *Stack {
	ret := &Stack{
		stack: make([]*SessionData, len(s.stack)),
	}
	for i, st := range s.stack {
		ret.stack[i] = st.Clone()
	}
	ret.base = ret.stack[0]
	return ret
}

// Replace replaces the current stack with the provided stack.
func (s *Stack) Replace(repl *Stack) {
	// Replace with a clone, as the same stack savepoint can be re-used.
	*s = *repl.Clone()
}

// Top returns the top element of the stack.
func (s *Stack) Top() *SessionData {
	if len(s.stack) == 0 {
		return nil
	}
	return s.stack[len(s.stack)-1]
}

// Base returns the bottom element of the stack.
// This is a non-racy structure, as the bottom element is always constant.
func (s *Stack) Base() *SessionData {
	return s.base
}

// Push pushes a SessionData element to the stack.
func (s *Stack) Push(elem *SessionData) {
	s.stack = append(s.stack, elem)
}

// PushTopClone pushes a copy of the top element to the stack.
func (s *Stack) PushTopClone() {
	if len(s.stack) == 0 {
		return
	}
	sd := s.stack[len(s.stack)-1]
	s.stack = append(s.stack, sd.Clone())
}

// Pop removes the top SessionData element from the stack.
func (s *Stack) Pop() error {
	if len(s.stack) <= 1 {
		return errors.AssertionFailedf("there must always be at least one element in the SessionData stack")
	}
	idx := len(s.stack) - 1
	s.stack[idx] = nil
	s.stack = s.stack[:idx]
	return nil
}

// PopN removes the top SessionData N elements from the stack.
func (s *Stack) PopN(n int) error {
	if len(s.stack)-n <= 0 {
		return errors.AssertionFailedf("there must always be at least one element in the SessionData stack")
	}
	// Explicitly unassign each pointer.
	for i := 0; i < n; i++ {
		s.stack[len(s.stack)-1-i] = nil
	}
	s.stack = s.stack[:len(s.stack)-n]
	return nil
}

// PopAll removes all except the base SessionData element from the stack.
func (s *Stack) PopAll() {
	// Explicitly unassign each pointer.
	for i := 1; i < len(s.stack); i++ {
		s.stack[i] = nil
	}
	s.stack = s.stack[:1]
}

// Elems returns all elements in the Stack.
func (s *Stack) Elems() []*SessionData {
	return s.stack
}

// Update performs a best-effort update of the field specified in 'variable' to
// the value specified in 'value'. Boolean indicating whether an update was
// performed is returned.
//
// The method only looks through the fields of embedded
// sessiondatapb.SessionData and sessiondatapb.LocalOnlySessionData structs and
// performs string-matching on the field name. Most custom types aren't
// supported.
func (s *SessionData) Update(variable, value string) bool {
	elem := reflect.ValueOf(&s.SessionData).Elem()
	if updateField(elem, variable, value) {
		return true
	}
	elem = reflect.ValueOf(&s.LocalOnlySessionData).Elem()
	return updateField(elem, variable, value)
}

// getValueToSet converts the provided value to reflect.Value based on the
// specified type. ok=false is returned if conversion isn't successful.
func getValueToSet(value, typeName string) (_ reflect.Value, ok bool) {
	switch typeName {
	case "bool":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(b), true
	case "float64":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(f), true
	case "int32":
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(int32(i)), true
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(i), true
	case "Duration":
		d, err := time.ParseDuration(value)
		if err != nil {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(d), true
	case "string":
		return reflect.ValueOf(value), true
	case "VectorizeExecMode":
		v, ok := sessiondatapb.VectorizeExecModeFromString(value)
		return reflect.ValueOf(v), ok
	}
	return reflect.Value{}, false
}

// updateField updates a single field in elem (which must be of Struct kind)
// that has the name specified in 'variable' to the value specified in 'value'.
// Boolean value indicates whether the update was successful.
func updateField(elem reflect.Value, variable, value string) bool {
	typ := elem.Type()
	for i := 0; i < elem.NumField(); i++ {
		if typ.Field(i).Name == variable {
			f := elem.Field(i)
			v, ok := getValueToSet(value, f.Type().Name())
			if !ok {
				return false
			}
			f.Set(v)
			return true
		}
	}
	return false
}
