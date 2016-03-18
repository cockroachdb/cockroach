// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
)

type isSessionTimezone interface {
	isSessionTimezone()
}

// SessionLocation ...
type SessionLocation struct {
	Location string
}

// SessionOffset ...
type SessionOffset struct {
	Offset int64
}

func (*SessionLocation) isSessionTimezone() {}
func (*SessionOffset) isSessionTimezone()   {}

// SessionTransaction ...
type SessionTransaction struct {
	// If missing, it means we're not inside a (KV) txn.
	Txn *roachpb.Transaction
	// txnAborted is set once executing a statement returned an error from KV.
	// While in this state, every subsequent statement must be rejected until a
	// COMMIT/ROLLBACK is seen.
	TxnAborted   bool
	UserPriority roachpb.UserPriority
	// Indicates that the transaction is mutating keys in the SystemConfig span.
	MutatesSystemConfig bool
}

// Session ...
type Session struct {
	Database string
	Syntax   int32
	// Info about the open transaction (if any).
	Txn                   SessionTransaction
	Timezone              isSessionTimezone
	DefaultIsolationLevel roachpb.IsolationType
}

func (s *Session) getLocation() (*time.Location, error) {
	switch t := s.Timezone.(type) {
	case nil:
		return time.UTC, nil
	case *SessionLocation:
		// TODO(vivek): Cache the location.
		return time.LoadLocation(t.Location)
	case *SessionOffset:
		return time.FixedZone("", int(t.Offset)), nil
	default:
		return nil, util.Errorf("unhandled timezone variant type %T", t)
	}
}
