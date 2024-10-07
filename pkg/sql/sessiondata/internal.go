// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondata

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// InternalExecutorOverride is used by the Executor interface
// to allow control over some of the session data.
type InternalExecutorOverride struct {
	// User represents the user that the query will run under.
	User username.SQLUsername
	// Database represents the default database for the query.
	Database string
	// ApplicationName represents the application that the query runs under.
	ApplicationName string
	// SearchPath represents the namespaces to search in.
	SearchPath *SearchPath
	// DatabaseIDToTempSchemaID represents the mapping for temp schemas used which
	// allows temporary schema resolution by ID.
	DatabaseIDToTempSchemaID map[uint32]uint32
	// QualityOfService represents the admission control priority level to use for
	// the internal execution request. Anything in the range -128 -> 127 may be
	// used as long as that value has a QoSLevel defined
	// (see QoSLevel.ValidateInternal).
	QualityOfService *sessiondatapb.QoSLevel
	// InjectRetryErrorsEnabled, if true, injects a transaction retry error
	// _after_ the statement has been processed by the execution engine and
	// _before_ the control flow is returned to the connExecutor state machine.
	//
	// The error will be injected (roughly speaking) three times (determined by
	// the numTxnRetryErrors constant in conn_executor_exec.go).
	//
	// For testing only.
	//
	// NB: this override applies only to the "top" internal executor, i.e. it
	// does **not** propagate further to "nested" executors that are spawned up
	// by the "top" executor.
	InjectRetryErrorsEnabled bool
	// OptimizerUseHistograms indicates whether we should use histograms for
	// cardinality estimation in the optimizer.
	// TODO(#102954): this should be removed when #102954 is fixed.
	OptimizerUseHistograms bool
	// MultiOverride, if set, is a comma-separated list of variable_name=value
	// overrides. For example, 'Database=foo,OptimizerUseHistograms=true'. These
	// overrides are performed on the best-effort basis - see SessionData.Update
	// for more details.
	MultiOverride string
	// AttributeToUser notifies the internal executor that the query is executed
	// directly on the user's behalf, and as such it should be included into
	// "external" / user-owned observability features (like SQL Activity page
	// and number of statements executed).
	//
	// Note that unlike other fields in this struct, this boolean doesn't
	// directly result in modification of the SessionData. Instead, it changes
	// the construction of the connExecutor used for the query.
	AttributeToUser bool
	// DisableChangefeedReplication, when true, disables changefeed events from
	// being emitted for changes to data made in a session. It is illegal to set
	// this option when using the internal executor with an outer txn.
	DisableChangefeedReplication bool
	// OriginIDForLogicalDataReplication is an identifier for the cluster that
	// originally wrote the data that are being written in this session. An
	// originID of 0 (the default) identifies a local write, 1 identifies a remote
	// write of unspecified origin, and 2+ are reserved to identify remote writes
	// from specific clusters.
	OriginIDForLogicalDataReplication uint32
	// OriginTimestampForLogicalDataReplication is the mvcc timestamp the data
	// written in this session were originally written with before being
	// replicated via Logical Data Replication. The creator of this internal
	// executor session is responsible for ensuring that every row it writes via
	// the internal executor had this origin timestamp.
	OriginTimestampForLogicalDataReplication hlc.Timestamp
	// PlanCacheMode, if set, overrides the plan_cache_mode session variable.
	PlanCacheMode *sessiondatapb.PlanCacheMode
	// GrowStackSize, if true, indicates that the connExecutor goroutine stack
	// should be grown to 32KiB right away.
	GrowStackSize bool
	// DisablePlanGists, if true, overrides the disable_plan_gists session var.
	DisablePlanGists bool
}

// NoSessionDataOverride is the empty InternalExecutorOverride which does not
// override any session data.
var NoSessionDataOverride = InternalExecutorOverride{}

// NodeUserSessionDataOverride is an InternalExecutorOverride which overrides
// the user to the NodeUser.
var NodeUserSessionDataOverride = InternalExecutorOverride{
	User: username.MakeSQLUsernameFromPreNormalizedString(username.NodeUser),
}

// NodeUserWithLowUserPrioritySessionDataOverride is an InternalExecutorOverride
// which overrides the user to the NodeUser and sets the quality of service to
// sessiondatapb.UserLow.
var NodeUserWithLowUserPrioritySessionDataOverride = InternalExecutorOverride{
	User:             username.MakeSQLUsernameFromPreNormalizedString(username.NodeUser),
	QualityOfService: &sessiondatapb.UserLowQoS,
}

// NodeUserWithBulkLowPriSessionDataOverride is an InternalExecutorOverride
// which overrides the user to the NodeUser and sets the quality of service to
// sessiondatapb.BulkLow.
var NodeUserWithBulkLowPriSessionDataOverride = InternalExecutorOverride{
	User:             username.MakeSQLUsernameFromPreNormalizedString(username.NodeUser),
	QualityOfService: &sessiondatapb.BulkLowQoS,
}
