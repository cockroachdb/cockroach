// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlinstance provides interfaces that will be exposed
// to interact with the sqlinstance subsystem.
// This subsystem will initialize and maintain a unique instance ID
// per SQL instance along with mapping of an active instance ID to
// its SQL address.
package sqlinstance

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/errors"
)

// InstanceInfo exposes information on a SQL instance such as ID, network address and
// the associated sqlliveness.SessionID.
type InstanceInfo struct {
	InstanceID   base.SQLInstanceID
	InstanceAddr string
	SessionID    sqlliveness.SessionID
}

// AddressResolver exposes API for retrieving the instance address and all live instances for a tenant.
type AddressResolver interface {
	// GetInstance returns the InstanceInfo for a pod given the instance ID.
	// Returns a NonExistentInstanceError if an instance with the given ID
	// does not exist.
	GetInstance(context.Context, base.SQLInstanceID) (InstanceInfo, error)
	// GetAllInstances returns a list of all SQL instances for the tenant.
	GetAllInstances(context.Context) ([]InstanceInfo, error)
}

// Provider is a wrapper around sqlinstance subsystem for external consumption.
type Provider interface {
	AddressResolver
	Instance(context.Context) (base.SQLInstanceID, error)
}

// NonExistentInstanceError can be returned if a SQL instance does not exist.
var NonExistentInstanceError = errors.Errorf("non existent SQL instance")
