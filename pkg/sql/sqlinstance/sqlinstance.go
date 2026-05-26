// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sqlinstance provides interfaces that will be exposed
// to interact with the sqlinstance subsystem.
// This subsystem will initialize and maintain a unique instance ID
// per SQL instance along with mapping of an active instance ID to
// its SQL address.
package sqlinstance

import (
	"context"
	"encoding/base64"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// InstanceInfo exposes information on a SQL instance such as ID, network
// address, the associated sqlliveness.SessionID, and the instance's locality.
type InstanceInfo struct {
	Region          []byte
	InstanceID      base.SQLInstanceID
	InstanceSQLAddr string
	InstanceRPCAddr string
	SessionID       sqlliveness.SessionID
	Locality        roachpb.Locality
	BinaryVersion   roachpb.Version
	IsDraining      bool
}

func (ii InstanceInfo) GetInstanceID() base.SQLInstanceID {
	return ii.InstanceID
}

func (ii InstanceInfo) GetLocality() roachpb.Locality {
	return ii.Locality
}

// SafeFormat implements redact.SafeFormatter.
func (ii InstanceInfo) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Printf(
		"Instance{RegionPrefix: %v, InstanceID: %d, SQLAddr: %v, RPCAddr: %v, SessionID: %s, Locality: %v, BinaryVersion: %v}",
		redact.SafeString(base64.StdEncoding.EncodeToString(ii.Region)),
		ii.InstanceID,
		ii.InstanceSQLAddr,
		ii.InstanceRPCAddr,
		ii.SessionID,
		ii.Locality,
		ii.BinaryVersion,
	)
}

// String implements fmt.Stringer.
func (ii InstanceInfo) String() string {
	return redact.Sprint(ii).StripMarkers()
}

var _ redact.SafeFormatter = InstanceInfo{}

// AddressResolver exposes API for retrieving the instance address and all live instances for a tenant.
type AddressResolver interface {
	// GetInstance returns the InstanceInfo for a pod given the instance ID.
	// Returns a NonExistentInstanceError if an instance with the given ID
	// does not exist.
	GetInstance(context.Context, base.SQLInstanceID) (InstanceInfo, error)
	// GetAllInstances returns a list of all SQL instances for the tenant.
	GetAllInstances(context.Context) ([]InstanceInfo, error)
}

// NonExistentInstanceError can be returned if a SQL instance does not exist.
var NonExistentInstanceError = errors.Errorf("non existent SQL instance")
