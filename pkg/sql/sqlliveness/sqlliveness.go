// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// Provider is a wrapper around the sqllivness subsystem for external
// consumption.
type Provider interface {
	Start(ctx context.Context)
	Metrics() metric.Struct
	Liveness

	// CachedReader returns a reader which only consults its local cache and
	// does not perform any RPCs in the IsAlive call.
	CachedReader() Reader
}

// Liveness exposes Reader and Instance interfaces.
type Liveness interface {
	Reader
	Instance
}

// String returns a hex-encoded version of the SessionID.
func (s SessionID) String() string {
	return hex.EncodeToString(encoding.UnsafeConvertStringToBytes(string(s)))
}

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

	// Expiration is the current expiration value for this Session. If the Session
	// expires, this function will return a zero-value timestamp.
	// Transactions run by this Instance which ensure that they commit before
	// this time will be assured that any resources claimed under this session
	// are known to be valid.
	Expiration() hlc.Timestamp
	// RegisterCallbackForSessionExpiry registers a callback to be executed when the session expires.
	RegisterCallbackForSessionExpiry(func(ctx context.Context))
}

// Reader abstracts over the state of session records.
type Reader interface {
	// IsAlive is used to query the liveness of a Session typically by another
	// Instance that is attempting to claim expired resources.
	IsAlive(context.Context, SessionID) (alive bool, err error)
}

// TestingKnobs contains test knobs for sqlliveness system behavior.
type TestingKnobs struct {
	SessionOverride func(ctx context.Context) (Session, error)
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

// NotStartedError can be returned from calls to the sqlliveness subsystem
// prior to its being started.
var NotStartedError = errors.Errorf("sqlliveness subsystem has not yet been started")
