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

	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/errors"
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
// NB: The values of the enums must be stable across releases.
type SerialNormalizationMode int64

const (
	// SerialUsesRowID means use INT NOT NULL DEFAULT unique_rowid().
	SerialUsesRowID SerialNormalizationMode = 0
	// SerialUsesVirtualSequences means create a virtual sequence and
	// use INT NOT NULL DEFAULT nextval(...).
	SerialUsesVirtualSequences SerialNormalizationMode = 1
	// SerialUsesSQLSequences means create a regular SQL sequence and
	// use INT NOT NULL DEFAULT nextval(...). Each call to nextval()
	// is a distributed call to kv. This minimizes the size of gaps
	// between successive sequence numbers (which occur due to
	// node failures or errors), but the multiple kv calls
	// can impact performance negatively.
	SerialUsesSQLSequences SerialNormalizationMode = 2
	// SerialUsesCachedSQLSequences is identical to SerialUsesSQLSequences with
	// the exception that nodes can cache sequence values. This significantly
	// reduces contention and distributed calls to kv, which results in better
	// performance. Gaps between sequences may be larger as a result of cached
	// values being lost to errors and/or node failures.
	SerialUsesCachedSQLSequences SerialNormalizationMode = 3
	// SerialUsesUnorderedRowID means use INT NOT NULL DEFAULT unordered_unique_rowid().
	SerialUsesUnorderedRowID SerialNormalizationMode = 4
)

func (m SerialNormalizationMode) String() string {
	switch m {
	case SerialUsesRowID:
		return "rowid"
	case SerialUsesUnorderedRowID:
		return "unordered_rowid"
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
	case "UNORDERED_ROWID":
		return SerialUsesUnorderedRowID, true
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
	// UseNewSchemaChangerUnsafe means that we attempt to use the new schema
	// changer for implemented statements including ones which aren't production
	// ready. Used for testing/development.
	UseNewSchemaChangerUnsafe
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
	case UseNewSchemaChangerUnsafe:
		return "unsafe"
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
	case "UNSAFE":
		return UseNewSchemaChangerUnsafe, true
	case "UNSAFE_ALWAYS":
		return UseNewSchemaChangerUnsafeAlways, true
	default:
		return 0, false
	}
}

// QoSLevel controls the level of admission control to use for new SQL requests.
type QoSLevel admission.WorkPriority

const (
	// SystemLow denotes the minimum system QoS level, which is not settable as a
	// session default_transaction_quality_of_service value.
	SystemLow = QoSLevel(admission.LowPri)

	// TTLStatsLow denotes a QoS level used internally by the TTL feature, which
	// is not settable as a session default_transaction_quality_of_service value.
	TTLStatsLow = QoSLevel(admission.TTLLowPri)

	// TTLLow denotes a QoS level used internally by the TTL feature, which is not
	// settable as a session default_transaction_quality_of_service value.
	TTLLow = QoSLevel(admission.TTLLowPri)

	// UserLow denotes an end user QoS level lower than the default.
	UserLow = QoSLevel(admission.UserLowPri)

	// Normal denotes an end user QoS level unchanged from the default.
	Normal = QoSLevel(admission.NormalPri)

	// UserHigh denotes an end user QoS level higher than the default.
	UserHigh = QoSLevel(admission.UserHighPri)

	// Locking denotes an internal increased priority for transactions that are
	// acquiring locks.
	Locking = QoSLevel(admission.LockingPri)

	// SystemHigh denotes the maximum system QoS level, which is not settable as a
	// session default_transaction_quality_of_service value.
	SystemHigh = QoSLevel(admission.HighPri)
)

const (
	// NormalName is the external session setting string value to use to mean
	// Normal QoS level.
	NormalName = "regular"

	// UserHighName is the external session setting string value to use to mean
	// UserHigh QoS level.
	UserHighName = "critical"

	// UserLowName is the external session setting string value to use to mean
	// UserLow QoS level.
	UserLowName = "background"

	// SystemHighName is the string value to display indicating a SystemHigh
	// QoS level.
	SystemHighName = "maximum"

	// SystemLowName is the string value to display indicating a SystemLow
	// QoS level.
	SystemLowName = "minimum"

	// TTLLowName is the string value to display indicating a TTLLow QoS level.
	TTLLowName = "ttl_low"

	// LockingName is the string value to display indicating a Locking QoS level.
	LockingName = "locking"
)

var qosLevelsDict = map[QoSLevel]string{
	SystemLow:  SystemLowName,
	TTLLow:     TTLLowName,
	UserLow:    UserLowName,
	Normal:     NormalName,
	UserHigh:   UserHighName,
	Locking:    LockingName,
	SystemHigh: SystemHighName,
}

// ParseQoSLevelFromString converts a string into a QoSLevel
func ParseQoSLevelFromString(val string) (_ QoSLevel, ok bool) {
	switch strings.ToUpper(val) {
	case strings.ToUpper(UserHighName):
		return UserHigh, true
	case strings.ToUpper(UserLowName):
		return UserLow, true
	case strings.ToUpper(NormalName):
		return Normal, true
	default:
		return 0, false
	}
}

// String prints the string representation of the
// default_transaction_quality_of_service session setting.
func (e QoSLevel) String() string {
	if name, ok := qosLevelsDict[e]; ok {
		return name
	}
	return fmt.Sprintf("%d", int(e))
}

// ToQoSLevelString interprets an int32 value as a QoSLevel and returns its
// String representation.
func ToQoSLevelString(value int32) string {
	if value > int32(SystemHigh) || value < int32(SystemLow) {
		return fmt.Sprintf("%d", value)
	}
	qosLevel := QoSLevel(value)
	return qosLevel.String()
}

// Validate checks for a valid user QoSLevel setting before returning it.
func (e QoSLevel) Validate() QoSLevel {
	switch e {
	case Normal, UserHigh, UserLow:
		return e
	default:
		panic(errors.AssertionFailedf("use of illegal user QoSLevel: %s", e.String()))
	}
}

// ValidateInternal checks for a valid internal QoSLevel setting before
// returning it.
func (e QoSLevel) ValidateInternal() QoSLevel {
	if _, ok := qosLevelsDict[e]; ok {
		return e
	}
	panic(errors.AssertionFailedf("use of illegal internal QoSLevel: %s", e.String()))
}
