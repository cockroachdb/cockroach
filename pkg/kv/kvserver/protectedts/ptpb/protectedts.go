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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// MakeClusterTarget returns a target, which when used in a Record, will
// protect the entire keyspace of the cluster.
func MakeClusterTarget() *Target {
	return &Target{&Target_Cluster{Cluster: &Target_ClusterTarget{}}}
}

// MakeTenantsTarget returns a target, which when used in a Record, will
// protect the keyspace of all tenants in ids.
func MakeTenantsTarget(ids []roachpb.TenantID) *Target {
	return &Target{&Target_Tenants{Tenants: &Target_TenantsTarget{IDs: ids}}}
}

// MakeSchemaObjectsTarget returns a target, which when used in a Record,
// will protect the keyspace of all schema objects (database/table).
func MakeSchemaObjectsTarget(ids descpb.IDs) *Target {
	return &Target{&Target_SchemaObjects{SchemaObjects: &Target_SchemaObjectsTarget{IDs: ids}}}
}

// String implements the stringer interface.
func (t Target) String() string {
	s := strings.Builder{}
	switch c := t.GetUnion().(type) {
	case *Target_Cluster:
		s.WriteString("ClusterTarget")
	case *Target_Tenants:
		s.WriteString("TenantsTarget:{")
		tenIDStr := make([]string, len(c.Tenants.IDs))
		for i, tenID := range c.Tenants.IDs {
			tenIDStr[i] = tenID.String()
		}
		s.WriteString(strings.Join(tenIDStr, ","))
		s.WriteString("}")
	case *Target_SchemaObjects:
		s.WriteString("SchemaObjectsTarget:{")
		descIDStr := make([]string, len(c.SchemaObjects.IDs))
		for i, descID := range c.SchemaObjects.IDs {
			descIDStr[i] = strconv.Itoa(int(descID))
		}
		s.WriteString(strings.Join(descIDStr, ","))
		s.WriteString("}")
	}
	return s.String()
}
