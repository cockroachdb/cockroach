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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// SessionData contains session parameters. They are all user-configurable.
// A SQL Session changes fields in SessionData through sql.sessionDataMutator.
type SessionData struct {
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
	// Location indicates the current time zone.
	Location *time.Location
	// SearchPath is a list of databases that will be searched for a table name
	// before the database. Currently, this is used only for SELECTs.
	// Names in the search path must have been normalized already.
	SearchPath SearchPath
	// User is the name of the user logged into the session.
	User string
	// SafeUpdates causes errors when the client
	// sends syntax that may have unwanted side effects.
	SafeUpdates bool
	// SequenceState gives access to the SQL sequences that have been manipulated
	// by the session.
	SequenceState *SequenceState
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
func DistSQLExecModeFromString(val string) DistSQLExecMode {
	switch strings.ToUpper(val) {
	case "OFF":
		return DistSQLOff
	case "AUTO":
		return DistSQLAuto
	case "ON":
		return DistSQLOn
	case "ALWAYS":
		return DistSQLAlways
	default:
		panic(fmt.Sprintf("unknown DistSQL mode %s", val))
	}
}
