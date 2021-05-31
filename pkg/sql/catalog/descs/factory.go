// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// collectionDeps are the node-level dependencies used by a Collection.
type collectionDeps struct {
	// settings are required to correctly resolve system.namespace accesses in
	// mixed version (19.2/20.1) clusters.
	// TODO(solon): This field could maybe be removed in 20.2.
	settings *cluster.Settings
	// leaseMgr manages acquiring and releasing per-descriptor leases.
	leaseMgr *lease.Manager
	// virtualSchemas optionally holds the virtual schemas.
	virtualSchemas catalog.VirtualSchemas
	// hydratedTables is node-level cache of table descriptors which utilize
	// user-defined types.
	hydratedTables *hydratedtables.Cache
}

// Factory constructs a Collection.
type Factory struct {
	collectionDeps
}

// NewFactory constructs a new *Factory.
func NewFactory(
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	hydratedTables *hydratedtables.Cache,
	virtualSchemas catalog.VirtualSchemas,
) *Factory {
	return &Factory{
		collectionDeps: collectionDeps{
			settings:       settings,
			leaseMgr:       leaseMgr,
			hydratedTables: hydratedTables,
			virtualSchemas: virtualSchemas,
		},
	}
}

// NewCollection constructs a new *Collection.
func (f *Factory) NewCollection(sd *sessiondata.SessionData) *Collection {
	c := f.MakeCollection(sd)
	return &c
}

// MakeCollection constructs a new Collection.
func (f *Factory) MakeCollection(sd *sessiondata.SessionData) Collection {
	return Collection{
		collectionDeps: f.collectionDeps,
		sessionData:    sd,
	}
}
