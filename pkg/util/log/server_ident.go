// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import "context"

// ServerIdentificationContextKey is the type of a context.Value key
// used to carry ServerIdentificationPayload values.
type ServerIdentificationContextKey struct{}

// ServerIdentificationPayload is the type of a context.Value payload
// associated with a ServerIdentificationContextKey.
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
	// IdentifyNodeID retrieves the KV node ID of the server.
	IdentifyNodeID
	// IdentifyInstanceID retrieves the SQL instance ID of the server.
	IdentifyInstanceID
	// IdentifyTenantID retrieves the tenant ID of the server.
	IdentifyTenantID
)

type idPayload struct {
	// the Cluster ID is reported on every new log file so as to ease
	// the correlation of panic reports with self-reported log files.
	clusterID string
	// the node ID is reported like the cluster ID, for the same reasons.
	// We avoid using roahcpb.NodeID to avoid a circular reference.
	nodeID string
	// ditto for the tenant ID.
	tenantID string
	// ditto for the SQL instance ID.
	sqlInstanceID string
}

func getIdentificationPayload(ctx context.Context) (res idPayload) {
	r := ctx.Value(ServerIdentificationContextKey{})
	if r == nil {
		return res
	}
	si, ok := r.(ServerIdentificationPayload)
	if !ok {
		return res
	}
	res.clusterID = si.ServerIdentityString(IdentifyClusterID)
	res.nodeID = si.ServerIdentityString(IdentifyNodeID)
	res.sqlInstanceID = si.ServerIdentityString(IdentifyInstanceID)
	res.tenantID = si.ServerIdentityString(IdentifyTenantID)
	return res
}
