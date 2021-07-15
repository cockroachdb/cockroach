// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlinstance provides interfaces that will be exposed
// to interact with the sqlinstance subsystem. This subsystem will
// initialize and maintain unique instance ids per SQL instance
// along with mapping of an active instance id to its http addresses.
package sqlinstance

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
)

// InstanceInfo exposes information on a SQL instance such as ID, network address and
// the associated sqlliveness.SessionID.
type InstanceInfo struct {
	instanceID   base.SQLInstanceID
	instanceAddr string
	sessionID    sqlliveness.SessionID
}

// AddressResolver exposes API for retrieving the instance address and all live instances for a tenant.
type AddressResolver interface {
	GetInstanceAddr(context.Context, base.SQLInstanceID) (string, error)
	GetAllInstances(context.Context) ([]InstanceInfo, error)
}

// Provider is a wrapper around sqlinstance subsystem for external consumption.
type Provider interface {
	AddressResolver
	Instance(context.Context) (base.SQLInstanceID, error)
}

// SessionExpiry executes SQL pod shutdown on sqlliveness.Session expiration.
type SessionExpiry func(ctx context.Context)

// NewSQLInstanceInfo constructs a new InstanceInfo.
func NewSQLInstanceInfo(
	instanceID base.SQLInstanceID, addr string, sessionID sqlliveness.SessionID,
) *InstanceInfo {
	return &InstanceInfo{
		instanceID:   instanceID,
		instanceAddr: addr,
		sessionID:    sessionID,
	}
}

// InstanceID returns the base.SQLInstanceID associated with the SQL instance.
func (i *InstanceInfo) InstanceID() base.SQLInstanceID {
	return i.instanceID
}

// InstanceAddr returns the SQL address associated with the SQL instance.
func (i *InstanceInfo) InstanceAddr() string {
	return i.instanceAddr
}

// SessionID returns the sqlliveness.SessionID associated with the SQL Instance.
func (i *InstanceInfo) SessionID() sqlliveness.SessionID {
	return i.sessionID
}
