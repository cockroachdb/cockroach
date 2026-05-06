// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package resourcegroup contains the top-level interfaces shared by
// the per-tenant reconciler that pushes resource group configuration
// changes to the host and the host-side subscriber that consumes
// them. Concrete implementations live in subpackages: rgreconciler,
// rgkvaccessor, rgsubscriber.
package resourcegroup

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// tenantResourceGroupsVersionGate is the cluster version that gates
// reconciliation: until it is active, the system.tenant_resource_groups
// table the host writer needs may not exist yet.
var tenantResourceGroupsVersionGate = clusterversion.V26_3_AddTenantResourceGroupsTable

// versionGatePollInterval is how often WaitForTenantResourceGroupsTable
// re-checks the cluster version while waiting for the gate.
const versionGatePollInterval = 30 * time.Second

// WaitForTenantResourceGroupsTable blocks until the cluster version
// that creates system.tenant_resource_groups is active, the stopper
// is quiescing, or ctx is cancelled.
func WaitForTenantResourceGroupsTable(
	ctx context.Context, settings *cluster.Settings, stopper *stop.Stopper,
) error {
	if settings.Version.IsActive(ctx, tenantResourceGroupsVersionGate) {
		return nil
	}
	log.Dev.Infof(ctx, "resourcegroup: waiting for cluster version %s", tenantResourceGroupsVersionGate)
	var t timeutil.Timer
	defer t.Stop()
	for {
		t.Reset(versionGatePollInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopper.ShouldQuiesce():
			return errors.New("server is shutting down")
		case <-t.C:
		}
		if settings.Version.IsActive(ctx, tenantResourceGroupsVersionGate) {
			return nil
		}
	}
}

// Pusher is what the per-tenant reconciler uses to forward a batch of
// upserts and deletes to the host's system.tenant_resource_groups
// table. It abstracts the difference between the system tenant
// (which writes locally) and an application tenant (which calls
// UpdateResourceGroups on the connector).
//
// Push is called once per rangefeed-frontier checkpoint and must
// block until the batch has been durably recorded on the host.
// Implementations are called from a single goroutine; they need not
// be safe for concurrent calls.
type Pusher interface {
	Push(
		ctx context.Context,
		upserts []*rgpb.ResourceGroupUpsert,
		deletes []*rgpb.ResourceGroupDelete,
	) error
}
