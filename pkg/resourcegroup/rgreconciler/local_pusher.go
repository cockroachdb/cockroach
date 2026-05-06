// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/resourcegroup"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// newLocalPusher returns a Pusher that writes directly to the host's
// system.tenant_resource_groups via Writer, without going through the
// UpdateResourceGroups RPC. Used by the system-tenant reconciler.
func newLocalPusher(w rgkvaccessor.Writer, tenantID roachpb.TenantID) resourcegroup.Pusher {
	return &localPusher{w: w, tenantID: tenantID}
}

type localPusher struct {
	w        rgkvaccessor.Writer
	tenantID roachpb.TenantID
}

// Push implements resourcegroup.Pusher.
func (p *localPusher) Push(
	ctx context.Context, upserts []*rgpb.ResourceGroupUpsert, deletes []*rgpb.ResourceGroupDelete,
) error {
	return p.w.Apply(ctx, p.tenantID, upserts, deletes)
}
