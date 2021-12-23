// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptpb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// MakeRecordClusterTarget returns a target, which when used in a Record, will
// protect the entire keyspace of the cluster.
func MakeRecordClusterTarget() *Target {
	return &Target{&Target_Cluster{Cluster: &Target_ClusterTarget{}}}
}

// MakeRecordTenantsTarget returns a target, which when used in a Record, will
// protect the keyspace of all tenants in ids.
func MakeRecordTenantsTarget(ids []roachpb.TenantID) *Target {
	return &Target{&Target_Tenants{Tenants: &Target_TenantsTarget{IDs: ids}}}
}

// MakeRecordSchemaObjectsTarget returns a target, which when used in a Record,
// will protect the keyspace of all schema objects (database/table).
func MakeRecordSchemaObjectsTarget(ids descpb.IDs) *Target {
	return &Target{&Target_SchemaObjects{&Target_SchemaObjectsTarget{IDs: ids}}}
}
