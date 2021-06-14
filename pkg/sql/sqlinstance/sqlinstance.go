// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlinstance provides interfaces to initialize unique
// instance ids for SQL pods and resolve network addresses
// for a pod given an instance id
package sqlinstance

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
)

// AddressResolver resolves http address mapping
// for a given SQL instance id
type AddressResolver interface {
	GetInstanceAddr(base.SQLInstanceID) (string, error)
}

// SQLInstance is a wrapper around the the SQLInstanceID
// type
type SQLInstance interface {
	InstanceID() base.SQLInstanceID
}

// Writer interface exposes methods necessary to create
// a new SQL instance id and associate an http address
// with it
type Writer interface {
	CreateInstance(sqlliveness.SessionID) (SQLInstance, error)
	SetInstanceAddr(base.SQLInstanceID, string)
}

// InstanceManager is a wrapper around the SQL instance
// initialization and management subsystem
type InstanceManager interface {
	AddressResolver
	Writer
	ShutdownInstance(context.Context, sqlliveness.SessionID)
}
