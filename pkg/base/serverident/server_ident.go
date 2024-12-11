// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverident

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
)

// SystemTenantID is the string representation of
// roachpb.SystemTenantID. Injected at initialization to avoid
// an import dependency cycle. See SetSystemTenantID.
var SystemTenantID string

// ServerIdentificationContextKey is the fast value key used to annotate a
// context with a ServerIdentificationPayload.
var ServerIdentificationContextKey = ctxutil.RegisterFastValueKey()

// ContextWithServerIdentification returns a context annotated with the provided
// server identity. Use ServerIdentificationFromContext(ctx) to retrieve it from
// the ctx later.
func ContextWithServerIdentification(
	ctx context.Context, serverID ServerIdentificationPayload,
) context.Context {
	return ctxutil.WithFastValue(ctx, ServerIdentificationContextKey, serverID)
}

// ServerIdentificationFromContext retrieves the server identity put in the
// context by ContextWithServerIdentification.
func ServerIdentificationFromContext(ctx context.Context) ServerIdentificationPayload {
	r := ctxutil.FastValue(ctx, ServerIdentificationContextKey)
	if r == nil {
		return nil
	}
	// TODO(radu): an interface-to-interface conversion is not great in a hot
	// path. Maybe the type should be just a func instead of an interface.
	return r.(ServerIdentificationPayload)
}

// ServerIdentificationPayload is the type of a context.Value payload
// associated with a serverIdentificationContextKey.
type ServerIdentificationPayload interface {
	// ServerIdentityString retrieves an identifier corresponding to the
	// given retrieval key. If there is no value known for a given key,
	// the method can return the empty string.
	ServerIdentityString(key ServerIdentificationKey) string
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
	// IdentifyTenantName retrieves the tenant name of the server.
	IdentifyTenantName
)

type IDPayload struct {
	// the Cluster ID is reported on every new log file so as to ease
	// the correlation of panic reports with self-reported log files.
	ClusterID string
	// the node ID is reported like the cluster ID, for the same reasons.
	// We avoid using roachpb.NodeID to avoid a circular reference.
	NodeID string
	// ditto for the tenant ID.
	TenantID string
	// ditto for tenant name.
	TenantName string
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
		TenantName:    si.ServerIdentityString(IdentifyTenantName),
	}
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
