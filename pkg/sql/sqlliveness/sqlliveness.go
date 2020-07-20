// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlliveness

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// SessionID represents an opaque identifier for a session. This ID should be
// globally unique.
type SessionID []byte

// Equals returns true if ids are equal.
func (s SessionID) Equals(other SessionID) bool {
	return bytes.Equal(s, other)
}

func (s SessionID) String() string {
	return hex.EncodeToString(s)
}

// SQLInstance represents a SQL tenant server instance and is responsible for
// maintaining at most once session for this instance and heart beating the
// current live one if it exists and otherwise creating a new  live one.
type SQLInstance interface {
	Session(context.Context) (Session, error)
}

type Session interface {
	ID() SessionID

	// Expiration is the current expiration value for this Session. If the Session
	// expires, this function will return a zero-value timestamp.
	// Transactions run by this Instance which ensure that they commit before
	// this time will be assured that any resources claimed under this session
	// are known to be valid.
	//
	// See discussion in Open Questions in
	// http://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200615_sql_liveness.md
	Expiration() hlc.Timestamp
}

// Storage abstracts over the set of sessions in the cluster.
type Storage interface {
	// IsAlive is used to query the liveness of a Session typically by another
	// SQLInstance that is attempting to claim expired resources.
	IsAlive(context.Context, *kv.Txn, SessionID) (alive bool, err error)
}
