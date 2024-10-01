// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package regions provides infrastructure to retrieve the regions available
// to a tenant.
package regions

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/errors"
)

// Provider implements sql.RegionsProvider.
type Provider struct {
	codec     keys.SQLCodec
	connector Connector
	txn       *kv.Txn
	descs     *descs.Collection
}

// NewProvider constructs a new Provider.
func NewProvider(
	codec keys.SQLCodec, connector Connector, txn *kv.Txn, descs *descs.Collection,
) *Provider {
	return &Provider{
		codec:     codec,
		connector: connector,
		txn:       txn,
		descs:     descs,
	}
}

// Connector is used to fetch the set of regions from the host cluster.
type Connector interface {
	Regions(context.Context, *serverpb.RegionsRequest) (*serverpb.RegionsResponse, error)
}

// GetSystemRegions gets all regions available from the host cluster via
// the underlying Connector.
func (p *Provider) GetSystemRegions(ctx context.Context) (*serverpb.RegionsResponse, error) {
	return p.connector.Regions(ctx, &serverpb.RegionsRequest{})
}

// GetRegions resolves the regions available to this client.
//
// If this is the system tenant, or this tenant has not been converted into a
// multi-region tenant (it doesn't have a multi-region system database), then
// the regions retrieved from the underlying Connector will be returned.
//
// If this is a multi-region tenant, the regions from the Connector will be
// filtered to include only regions in the system database. If any regions in
// the system database are not present in the response from the host, they
// will be added.
func (p *Provider) GetRegions(ctx context.Context) (*serverpb.RegionsResponse, error) {
	regions, err := p.GetSystemRegions(ctx)
	if err != nil {
		return nil, err
	}
	if p.codec.ForSystemTenant() {
		return regions, nil
	}
	tenantRegions, err := getTenantRegions(ctx, p.txn, p.descs)
	if err != nil ||
		// If there are no tenant regions, return all the regions. This happens
		// if the tenant is not a multi-region tenant.
		len(tenantRegions) == 0 {
		return regions, err
	}

	for region := range regions.Regions {
		if _, ok := tenantRegions[region]; !ok {
			delete(regions.Regions, region)
		}
	}

	// Ensure that every tenant region appears in the output, even if the region
	// doesn't appear in the host region.
	//
	// It's weird for a tenant to have regions that don't have any zones in the
	// host, but it's certainly possible, and those regions should show up.
	for region := range tenantRegions {
		if _, ok := regions.Regions[region]; !ok {
			regions.Regions[region] = &serverpb.RegionsResponse_Region{
				Zones: []string{},
			}
		}
	}
	return regions, nil
}

// getTenantRegions fetches the multi-region enum corresponding to the system
// database of the current tenant, if that tenant is a multi-region tenant.
// It returns nil, nil if the tenant is not a multi-region tenant.
func getTenantRegions(
	ctx context.Context, txn *kv.Txn, descs *descs.Collection,
) (RegionSet, error) {
	systemDatabase, err := descs.ByIDWithLeased(txn).Get().Database(ctx, keys.SystemDatabaseID)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to resolve system database for regions",
		)
	}
	return GetDatabaseRegions(ctx, txn, systemDatabase, descs)
}
