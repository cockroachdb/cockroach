// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondatapb

import (
	"fmt"
	"strings"
)

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
