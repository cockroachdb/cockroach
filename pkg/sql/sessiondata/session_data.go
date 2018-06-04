// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sessiondata

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// SessionData contains session parameters. They are all user-configurable.
// A SQL Session changes fields in SessionData through sql.sessionDataMutator.
type SessionData struct {
	// ApplicationName is the name of the application running the
	// current session. This can be used for logging and per-application
	// statistics.
	ApplicationName string
	// Database indicates the "current" database for the purpose of
	// resolving names. See searchAndQualifyDatabase() for details.
	Database string
	// DefaultIsolationLevel indicates the default isolation level of
	// newly created transactions.
	DefaultIsolationLevel enginepb.IsolationType
	// DefaultReadOnly indicates the default read-only status of newly created
	// transactions.
	DefaultReadOnly bool
	// DistSQLMode indicates whether to run queries using the distributed
	// execution engine.
	DistSQLMode DistSQLExecMode
	// LookupJoinEnabled indicates whether the planner should try and plan a
	// lookup join where the left side is scanned and index lookups are done on
	// the right side. Will emit a warning if a lookup join can't be planned.
	LookupJoinEnabled bool
	// Location indicates the current time zone.
	Location *time.Location
	// OptimizerMode indicates whether to use the experimental optimizer for
	// query planning.
	OptimizerMode OptimizerMode
	// SearchPath is a list of databases that will be searched for a table name
	// before the database. Currently, this is used only for SELECTs.
	// Names in the search path must have been normalized already.
	SearchPath SearchPath
	// StmtTimeout is the duration a query is permitted to run before it is
	// canceled by the session. If set to 0, there is no timeout.
	StmtTimeout time.Duration
	// User is the name of the user logged into the session.
	User string
	// SafeUpdates causes errors when the client
	// sends syntax that may have unwanted side effects.
	SafeUpdates bool
	RemoteAddr  net.Addr
	// ZigzagJoinEnabled indicates whether the planner should try and plan a
	// zigzag join. Will emit a warning if a zigzag join can't be planned.
	ZigzagJoinEnabled bool

	// BytesEncodeFormat indicates how to encode byte arrays when converting
	// to string.
	BytesEncodeFormat BytesEncodeFormat

	// SequenceState gives access to the SQL sequences that have been manipulated
	// by the session.
	SequenceState *SequenceState
}

// Copy performs a deep copy of SessionData.
func (s *SessionData) Copy() SessionData {
	cp := *s
	cp.SequenceState = s.SequenceState.copy()
	return cp
}

// BytesEncodeFormat controls which format to use for BYTES->STRING
// conversions.
type BytesEncodeFormat int

const (
	// BytesEncodeHex uses the hex format: e'abc\n'::BYTES::STRING -> '\x61626312'.
	// This is the default, for compatibility with PostgreSQL.
	BytesEncodeHex BytesEncodeFormat = iota
	// BytesEncodeEscape uses the escaped format: e'abc\n'::BYTES::STRING -> 'abc\012'.
	BytesEncodeEscape
	// BytesEncodeBase64 uses base64 encoding.
	BytesEncodeBase64
)

func (f BytesEncodeFormat) String() string {
	switch f {
	case BytesEncodeHex:
		return "hex"
	case BytesEncodeEscape:
		return "escape"
	case BytesEncodeBase64:
		return "base64"
	default:
		return fmt.Sprintf("invalid (%d)", f)
	}
}

// BytesEncodeFormatFromString converts a string into a BytesEncodeFormat.
func BytesEncodeFormatFromString(val string) (_ BytesEncodeFormat, ok bool) {
	switch strings.ToUpper(val) {
	case "HEX":
		return BytesEncodeHex, true
	case "ESCAPE":
		return BytesEncodeEscape, true
	case "BASE64":
		return BytesEncodeBase64, true
	default:
		return -1, false
	}
}

// DistSQLExecMode controls if and when the Executor uses DistSQL.
type DistSQLExecMode int64

const (
	// DistSQLOff means that we never use distSQL.
	DistSQLOff DistSQLExecMode = iota
	// DistSQLAuto means that we automatically decide on a case-by-case basis if
	// we use distSQL.
	DistSQLAuto
	// DistSQLOn means that we use distSQL for queries that are supported.
	DistSQLOn
	// DistSQLAlways means that we only use distSQL; unsupported queries fail.
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

// OptimizerMode controls if and when the Executor uses the optimizer.
type OptimizerMode int64

const (
	// OptimizerOff means that we don't use the optimizer.
	OptimizerOff = iota
	// OptimizerOn means that we use the optimizer for all supported statements.
	OptimizerOn
	// OptimizerLocal means that we use the optimizer for all supported
	// statements, but we don't try to distribute the resulting plan.
	OptimizerLocal
	// OptimizerAlways means that we attempt to use the optimizer always, even
	// for unsupported statements which result in errors. This mode is useful
	// for testing.
	OptimizerAlways
)

func (m OptimizerMode) String() string {
	switch m {
	case OptimizerOff:
		return "off"
	case OptimizerOn:
		return "on"
	case OptimizerLocal:
		return "local"
	case OptimizerAlways:
		return "always"
	default:
		return fmt.Sprintf("invalid (%d)", m)
	}
}

// OptimizerModeFromString converts a string into a OptimizerMode
func OptimizerModeFromString(val string) (_ OptimizerMode, ok bool) {
	switch strings.ToUpper(val) {
	case "OFF":
		return OptimizerOff, true
	case "ON":
		return OptimizerOn, true
	case "LOCAL":
		return OptimizerLocal, true
	case "ALWAYS":
		return OptimizerAlways, true
	default:
		return 0, false
	}
}
