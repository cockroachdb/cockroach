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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
)

// CollectionFactory is used to construct a new Collection.
type CollectionFactory struct {
	settings       *cluster.Settings
	codec          keys.SQLCodec
	leaseMgr       *lease.Manager
	virtualSchemas catalog.VirtualSchemas
	hydratedTables *hydratedtables.Cache
	systemDatabase *systemDatabaseNamespaceCache
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
		codec:          leaseMgr.Codec(),
		leaseMgr:       leaseMgr,
		virtualSchemas: virtualSchemas,
		hydratedTables: hydratedTables,
		systemDatabase: newSystemDatabaseNamespaceCache(leaseMgr.Codec()),
	}
}

// NewBareBonesCollectionFactory constructs a new CollectionFactory which holds
// onto a minimum of dependencies needed to construct an operable Collection.
func NewBareBonesCollectionFactory(
	settings *cluster.Settings, codec keys.SQLCodec,
) *CollectionFactory {
	return &CollectionFactory{
		settings: settings,
		codec:    codec,
	}
}

// MakeCollection constructs a Collection for the purposes of embedding.
func (cf *CollectionFactory) MakeCollection(
	ctx context.Context, temporarySchemaProvider TemporarySchemaProvider,
) Collection {
	return makeCollection(ctx, cf.leaseMgr, cf.settings, cf.codec, cf.hydratedTables, cf.systemDatabase,
		cf.virtualSchemas, temporarySchemaProvider)
}

// NewCollection constructs a new Collection.
func (cf *CollectionFactory) NewCollection(
	ctx context.Context, temporarySchemaProvider TemporarySchemaProvider,
) *Collection {
	c := cf.MakeCollection(ctx, temporarySchemaProvider)
	return &c
}
