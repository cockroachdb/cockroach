// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment

import (
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Attributes captures the per-execution metadata that the enrichment
// subsystem associates with a SQL statement at execution time and later
// applies to ASH samples. All fields are populated at the gateway when
// the execution starts; downstream readers (the local sampler or remote
// enricher RPCs) read them by Key.
//
// The identifier fields (TxnID, SessionID) are stored as their typed
// value forms rather than as pre-encoded strings so that PutExecution
// on the gateway hot path does not have to call .String() on them. The
// hex-encoding work moves to the GetASHEnrichmentData RPC handler,
// where it only happens for samples the enricher actually asks about.
type Attributes struct {
	// AppName is the application_name session setting.
	AppName string
	// Database is the current database for the session.
	Database string
	// User is the user the session is authenticated as.
	User string
	// Query is the raw SQL text of the statement. May contain PII
	// (literal values from the user's query); production deployments
	// should treat this field as sensitive.
	Query string
	// PlanGist is the optimizer plan gist for the execution, in the
	// raw byte form produced by the optimizer.
	PlanGist []byte
	// CanaryStats is true if the execution used canary statistics.
	CanaryStats bool
	// TxnID is the transaction's UUID. Stored as a value to avoid an
	// allocation on the PutExecution hot path; string-encoded only at
	// the RPC response boundary.
	TxnID uuid.UUID
	// SessionID is the session's clusterunique.ID. Stored as a value
	// for the same reason as TxnID.
	SessionID clusterunique.ID
}

// entry is the cache's internal record type: the key followed by its
// attribute payload. entry is laid out contiguously in fixed-size blocks
// to minimize allocations on the hot path.
type entry struct {
	Key   clusterunique.ID
	Attrs Attributes
}

// valid reports whether an entry slot is occupied. The zero value of
// clusterunique.ID (Hi == 0 && Lo == 0) is never produced by GenerateID
// in practice (the HLC wall time is always non-zero), so we use it as
// the empty sentinel.
func (e entry) valid() bool {
	return e.Key.Hi != 0 || e.Key.Lo != 0
}
