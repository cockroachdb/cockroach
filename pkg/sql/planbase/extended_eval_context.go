// Copyright 2025 The CockroachDB Software License
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package planbase

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// ExtendedEvalContextI is the interface for extended evaluation context.
// This interface breaks the circular dependency - the concrete implementation
// is sql.extendedEvalContext in pkg/sql.
type ExtendedEvalContextI interface {
	// EvalContext returns the underlying eval.Context.
	EvalContext() *eval.Context

	// SessionData returns the session data.
	SessionData() *sessiondata.SessionData

	// GetExecCfg returns the executor configuration.
	// Returns interface{} to avoid circular dependency - cast to *ExecutorConfig in pkg/sql
	// Note: Named GetExecCfg (not ExecCfg) to avoid collision with the ExecCfg field in extendedEvalContext
	GetExecCfg() interface{}

	// TxnIsSingleStmt returns whether the transaction is a single statement.
	TxnIsSingleStmt() bool

	// GetTracing returns the tracing interface.
	GetTracing() interface{}

	// ValidateDbZoneConfig returns the pointer to the validateDbZoneConfig flag.
	ValidateDbZoneConfig() *bool

	// GetTxnModesSetter returns the transaction modes setter.
	GetTxnModesSetter() interface{}

	// GetSettings returns the cluster settings.
	GetSettings() interface{}

	// GetSQLStatusServer returns the SQL status server.
	GetSQLStatusServer() interface{}

	// GetStmtTimestamp returns the statement timestamp.
	GetStmtTimestamp() interface{}

	// GetSessionID returns the session ID.
	GetSessionID() interface{}

	// GetJobs returns the jobs collection.
	GetJobs() interface{}

	// GetAsOfSystemTime returns the AS OF SYSTEM TIME timestamp.
	GetAsOfSystemTime() interface{}

	// SetTxnTimestamp sets the transaction timestamp.
	SetTxnTimestamp(interface{})

	// GetDescIDGenerator returns the descriptor ID generator.
	GetDescIDGenerator() interface{}

	// GetTestingKnobs returns the testing knobs.
	GetTestingKnobs() interface{}

	// Additional methods can be added here as needed for planNode implementations
}
