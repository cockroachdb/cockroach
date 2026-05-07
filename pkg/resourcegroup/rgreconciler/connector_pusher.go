// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
)

// newConnectorPusher returns a Pusher that calls UpdateResourceGroups
// on the supplied tenant connector. Used by an application-tenant
// reconciler to forward changes to the host.
func newConnectorPusher(connector kvtenant.Connector) resourcegroup.Pusher {
	return &connectorPusher{connector: connector}
}

type connectorPusher struct {
	connector kvtenant.Connector
}

// Push implements resourcegroup.Pusher.
func (p *connectorPusher) Push(
	ctx context.Context, upserts []*rgpb.ResourceGroupUpsert, deletes []*rgpb.ResourceGroupDelete,
) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}
	req := &rgpb.UpdateResourceGroupsRequest{
		Upserts: make([]rgpb.ResourceGroupUpsert, len(upserts)),
		Deletes: make([]rgpb.ResourceGroupDelete, len(deletes)),
	}
	for i, u := range upserts {
		req.Upserts[i] = *u
	}
	for i, d := range deletes {
		req.Deletes[i] = *d
	}
	_, err := p.connector.UpdateResourceGroups(ctx, req)
	return err
}

// Replace implements resourcegroup.Pusher.
func (p *connectorPusher) Replace(ctx context.Context, upserts []*rgpb.ResourceGroupUpsert) error {
	req := &rgpb.UpdateResourceGroupsRequest{
		Replace: true,
		Upserts: make([]rgpb.ResourceGroupUpsert, len(upserts)),
	}
	for i, u := range upserts {
		req.Upserts[i] = *u
	}
	_, err := p.connector.UpdateResourceGroups(ctx, req)
	return err
}
