// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rgpb contains the wire types for the UpdateResourceGroups
// RPC, by which a tenant's reconciler job pushes changes to its
// system.resource_groups table to the host's
// system.tenant_resource_groups table.
package rgpb
