// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondata

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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
}

// NoSessionDataOverride is the empty InternalExecutorOverride which does not
// override any session data.
var NoSessionDataOverride = InternalExecutorOverride{}

// NodeUserSessionDataOverride is an InternalExecutorOverride which overrides
// the user to the NodeUser.
var NodeUserSessionDataOverride = InternalExecutorOverride{
	User: username.MakeSQLUsernameFromPreNormalizedString(username.NodeUser),
}

// RootUserSessionDataOverride is an InternalExecutorOverride which overrides
// the user to the RootUser.
var RootUserSessionDataOverride = InternalExecutorOverride{
	User: username.MakeSQLUsernameFromPreNormalizedString(username.RootUser),
}

// NodeUserWithLowUserPrioritySessionDataOverride is an InternalExecutorOverride
// which overrides the user to the NodeUser and sets the quality of service to
// sessiondatapb.UserLow.
var NodeUserWithLowUserPrioritySessionDataOverride = InternalExecutorOverride{
	User:             username.MakeSQLUsernameFromPreNormalizedString(username.NodeUser),
	QualityOfService: &sessiondatapb.UserLowQoS,
}
