// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
