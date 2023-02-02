// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverident

import "context"

// SystemTenantID is the string representation of
// roachpb.SystemTenantID. Injected at initialization to avoid
// an import dependency cycle. See SetSystemTenantID.
var SystemTenantID string

// ServerIdentificationContextKey is the type of a context.Value key
// used to carry ServerIdentificationPayload values.
type ServerIdentificationContextKey struct{}

// ContextWithServerIdentification returns a context annotated with the provided
// server identity. Use ServerIdentificationFromContext(ctx) to retrieve it from
// the ctx later.
func ContextWithServerIdentification(
	ctx context.Context, serverID ServerIdentificationPayload,
) context.Context {
	return context.WithValue(ctx, ServerIdentificationContextKey{}, serverID)
}

// ServerIdentificationFromContext retrieves the server identity put in the
// context by ContextWithServerIdentification.
func ServerIdentificationFromContext(ctx context.Context) ServerIdentificationPayload {
	r := ctx.Value(ServerIdentificationContextKey{})
	if r == nil {
		return nil
	}
	return r.(ServerIdentificationPayload)
}

// ServerIdentificationPayload is the type of a context.Value payload
// associated with a ServerIdentificationContextKey.
type ServerIdentificationPayload interface {
	// ServerIdentityString retrieves an identifier corresponding to the
	// given retrieval key. If there is no value known for a given key,
	// the method can return the empty string.
	ServerIdentityString(key ServerIdentificationKey) string

	// TenantID returns the roachpb.TenantID identifying the server. It's returned
	// as an interface{} because the log pkg cannot depend on roachpb (and the log pkg
	// is the one putting the ServerIdentificationPayload into the ctx, so it makes
	// sense for this interface to live in log).
	//
	// Note that this tenant ID should not be confused with the one put in the
	// context by roachpb.NewContextForTenant(): that one is used by a server
	// handling an RPC call, referring to the tenant that's the client of the RPC.
	TenantID() interface{}
}

// ServerIdentificationKey represents a possible parameter to the
// ServerIdentityString() method in ServerIdentificationPayload.
type ServerIdentificationKey int

const (
	// IdentifyClusterID retrieves the cluster ID of the server.
	IdentifyClusterID ServerIdentificationKey = iota
	// IdentifyKVNodeID retrieves the KV node ID of the server.
	IdentifyKVNodeID
	// IdentifyInstanceID retrieves the SQL instance ID of the server.
	IdentifyInstanceID
	// IdentifyTenantID retrieves the tenant ID of the server.
	IdentifyTenantID
)

type IDPayload struct {
	// the Cluster ID is reported on every new log file so as to ease
	// the correlation of panic reports with self-reported log files.
	ClusterID string
	// the node ID is reported like the cluster ID, for the same reasons.
	// We avoid using roahcpb.NodeID to avoid a circular reference.
	NodeID string
	// ditto for the tenant ID.
	//
	// NB: Use TenantID() to access/read this value to take advantage
	// of default behaviors.
	TenantIDInternal string
	// ditto for the SQL instance ID.
	SQLInstanceID string
}

func GetIdentificationPayload(ctx context.Context) IDPayload {
	si := ServerIdentificationFromContext(ctx)
	if si == nil {
		return IDPayload{}
	}
	return IDPayload{
		ClusterID:        si.ServerIdentityString(IdentifyClusterID),
		NodeID:           si.ServerIdentityString(IdentifyKVNodeID),
		SQLInstanceID:    si.ServerIdentityString(IdentifyInstanceID),
		TenantIDInternal: si.ServerIdentityString(IdentifyTenantID),
	}
}

// TenantID returns the tenant ID associated with this idPayload.
// if the idPayload has no tenant ID set, we default to the system
// tenant ID. NB: This function should never return an empty string.
func (ip IDPayload) TenantID() string {
	if ip.TenantIDInternal == "" {
		return SystemTenantID
	}
	return ip.TenantIDInternal
}

// SetSystemTenantID is used to set the string representation of
// roachpb.SystemTenantID at initialization to avoid an import dependency cycle.
// We need this value so we can tag each idPayload with the SystemTenantID by
// default if no ServerIdentificationPayload is found in the context accompanying
// the log entry, or if the ServerIdentificationPayload is missing a tenant ID.
//
// Panics if the value has already been set.
func SetSystemTenantID(sysTenantID string) {
	if SystemTenantID != "" {
		panic("programming error: system tenant ID log tag value already set")
	}
	SystemTenantID = sysTenantID
}
