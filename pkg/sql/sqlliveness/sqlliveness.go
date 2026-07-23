// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sqlliveness provides interfaces to associate resources at the SQL
// level with tenant SQL processes.
//
// For more info see:
// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200615_sql_liveness.md
package sqlliveness

import (
	"context"
	"encoding/hex"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// SessionID represents an opaque identifier for a session. This ID should be
// globally unique. It is a string so that it can be used as a map key but it
// may not be a well-formed UTF8 string.
type SessionID string

// Provider is a wrapper around the sqlliveness subsystem for external
// consumption.
type Provider interface {
	Instance
	StorageReader

	// Start starts the sqlliveness subsystem. regionPhysicalRep should
	// represent the physical representation of the current process region
	// stored in the multi-region enum type associated with the system
	// database.
	Start(ctx context.Context, regionPhysicalRep []byte)

	// Release delete's the sqlliveness session managed by the provider. This
	// should be called near the end of the drain process, after the server has
	// no running tasks that depend on the session.
	Release(ctx context.Context) (SessionID, error)

	// PauseLivenessHeartbeat prevents the sqlliveness session from being extended
	// into the future, which should be done if some key system table like system.leases
	// is inaccessible.
	PauseLivenessHeartbeat(ctx context.Context)

	// UnpauseLivenessHeartbeat resumes sqlliveness session extensions.
	UnpauseLivenessHeartbeat(ctx context.Context)

	// Metrics returns a metric.Struct which holds metrics for the provider.
	Metrics() metric.Struct
}

// StorageReader provides access to Readers which either block or do not
// block.
type StorageReader interface {
	// BlockingReader returns a Reader which only synchronously
	// checks whether a session is alive if it does not have any
	// cached information which implies that it currently is.
	BlockingReader() Reader

	// CachedReader returns a reader which only consults its local cache and
	// does not perform any RPCs in the IsAlive call.
	CachedReader() Reader
}

// String returns a hex-encoded version of the SessionID.
func (s SessionID) String() string {
	return hex.EncodeToString(encoding.UnsafeConvertStringToBytes(string(s)))
}

// SafeValue implements the redact.SafeValue interface.
func (s SessionID) SafeValue() {}

// UnsafeBytes returns a byte slice representation of the ID. It is unsafe to
// modify this byte slice. This method is exposed to ease storing the session
// ID as bytes in SQL.
func (s SessionID) UnsafeBytes() []byte {
	return encoding.UnsafeConvertStringToBytes(string(s))
}

// Instance represents a SQL tenant server instance and is responsible for
// maintaining at most once session for this instance and heart beating the
// current live one if it exists and otherwise creating a new live one.
type Instance interface {
	Session(context.Context) (Session, error)
}

// Session represents a SQL instance lock with expiration.
type Session interface {
	ID() SessionID

	// Start is the start timestamp for this session. We offer disjointness over
	// session intervals, so if combined with Expiration() below, callers can
	// ensure that transactions run by this Instance (if committing within the
	// [start, expiration)), are disjoint with others committing within their
	// session intervals.
	Start() hlc.Timestamp

	// Expiration is the current expiration value for this Session. If the Session
	// expires, this function will return a zero-value timestamp.
	// Transactions run by this Instance which ensure that they commit before
	// this time will be assured that any resources claimed under this session
	// are known to be valid.
	Expiration() hlc.Timestamp
}

// Reader abstracts over the state of session records.
type Reader interface {
	// IsAlive is used to query the liveness of a Session typically by another
	// Instance that is attempting to claim expired resources.
	IsAlive(context.Context, SessionID) (alive bool, err error)
}

// TestingKnobs contains test knobs for sqlliveness system behavior.
type TestingKnobs struct {
	// SessionOverride is used to override the returned session.
	// If it returns nil, nil the underlying instance will be used.
	SessionOverride func(ctx context.Context) (Session, error)
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

// NotStartedError can be returned from calls to the sqlliveness subsystem
// prior to its being started.
var NotStartedError = errors.Errorf("sqlliveness subsystem has not yet been started")
