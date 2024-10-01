// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqllivenesstestutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type alwaysAliveSession string

// NewAlwaysAliveSession constructs and returns a session that is forever alive
// for testing purposes.
func NewAlwaysAliveSession(name string) sqlliveness.Session {
	return alwaysAliveSession(name)
}

// ID implements the sqlliveness.Session interface.
func (f alwaysAliveSession) ID() sqlliveness.SessionID {
	return sqlliveness.SessionID(f)
}

// Expiration implements the sqlliveness.Session interface.
func (f alwaysAliveSession) Expiration() hlc.Timestamp { return hlc.MaxTimestamp }

// Start implements the sqlliveness.Session interface.
func (f alwaysAliveSession) Start() hlc.Timestamp { return hlc.MinTimestamp }
