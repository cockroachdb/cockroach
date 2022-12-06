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

// ServerIdentificationContextKey is the type of a context.Value key
// used to carry ServerIdentificationPayload values.
type ServerIdentificationContextKey struct{}

func ContextWithServerIdentification(
	ctx context.Context, serverID ServerIdentificationPayload,
) context.Context {
	return context.WithValue(ctx, ServerIdentificationContextKey{}, serverID)
}

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
	TenantID string
	// ditto for the SQL instance ID.
	SQLInstanceID string
}

func GetIdentificationPayload(ctx context.Context) IDPayload {
	si := ServerIdentificationFromContext(ctx)
	if si == nil {
		return IDPayload{}
	}
	return IDPayload{
		ClusterID:     si.ServerIdentityString(IdentifyClusterID),
		NodeID:        si.ServerIdentityString(IdentifyKVNodeID),
		SQLInstanceID: si.ServerIdentityString(IdentifyInstanceID),
		TenantID:      si.ServerIdentityString(IdentifyTenantID),
	}
}
