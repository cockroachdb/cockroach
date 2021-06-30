// Copyright 2021 The Cockroach Authors.
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

// CollectionFactory is used to construct a new Collection.
type CollectionFactory struct {
	settings       *cluster.Settings
	leaseMgr       *lease.Manager
	virtualSchemas catalog.VirtualSchemas
	hydratedTables *hydratedtables.Cache
}

// NewCollectionFactory constructs a new CollectionFactory which holds onto
// the node-level dependencies needed to construct a Collection.
func NewCollectionFactory(
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	virtualSchemas catalog.VirtualSchemas,
	hydratedTables *hydratedtables.Cache,
) *CollectionFactory {
	return &CollectionFactory{
		settings:       settings,
		leaseMgr:       leaseMgr,
		virtualSchemas: virtualSchemas,
		hydratedTables: hydratedTables,
	}
}

// MakeCollection constructs a Collection for the purposes of embedding.
func (cf *CollectionFactory) MakeCollection(sd *sessiondata.SessionData) Collection {
	return makeCollection(
		cf.leaseMgr, cf.settings, cf.hydratedTables, cf.virtualSchemas, sd,
	)
}

// NewCollection constructs a new Collection.
func (cf *CollectionFactory) NewCollection(sd *sessiondata.SessionData) *Collection {
	c := cf.MakeCollection(sd)
	return &c
}
