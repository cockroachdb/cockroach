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
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// CollectionFactory is used to construct a new Collection.
type CollectionFactory struct {
	settings           *cluster.Settings
	codec              keys.SQLCodec
	leaseMgr           *lease.Manager
	virtualSchemas     catalog.VirtualSchemas
	hydrated           *hydrateddesc.Cache
	systemDatabase     *systemDatabaseNamespaceCache
	spanConfigSplitter spanconfig.Splitter
	spanConfigLimiter  spanconfig.Limiter
	defaultMonitor     *mon.BytesMonitor
}

// NewCollectionFactory constructs a new CollectionFactory which holds onto
// the node-level dependencies needed to construct a Collection.
func NewCollectionFactory(
	ctx context.Context,
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	virtualSchemas catalog.VirtualSchemas,
	hydrated *hydrateddesc.Cache,
	spanConfigSplitter spanconfig.Splitter,
	spanConfigLimiter spanconfig.Limiter,
) *CollectionFactory {
	return &CollectionFactory{
		settings:           settings,
		codec:              leaseMgr.Codec(),
		leaseMgr:           leaseMgr,
		virtualSchemas:     virtualSchemas,
		hydrated:           hydrated,
		systemDatabase:     newSystemDatabaseNamespaceCache(leaseMgr.Codec()),
		spanConfigSplitter: spanConfigSplitter,
		spanConfigLimiter:  spanConfigLimiter,
		defaultMonitor: mon.NewUnlimitedMonitor(ctx, "CollectionFactoryDefaultUnlimitedMonitor",
			mon.MemoryResource, nil /* curCount */, nil, /* maxHist */
			0 /* noteworthy */, settings),
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
	ctx context.Context, temporarySchemaProvider TemporarySchemaProvider, monitor *mon.BytesMonitor,
) Collection {
	if monitor == nil {
		// If an upstream monitor is not provided, the default, unlimited monitor will be used.
		// All downstream resource allocation/releases on this default monitor will then be no-ops.
		monitor = cf.defaultMonitor
	}

	return makeCollection(ctx, cf.leaseMgr, cf.settings, cf.codec, cf.hydrated, cf.systemDatabase,
		cf.virtualSchemas, temporarySchemaProvider, monitor)
}

// NewCollection constructs a new Collection.
func (cf *CollectionFactory) NewCollection(
	ctx context.Context, temporarySchemaProvider TemporarySchemaProvider,
) *Collection {
	c := cf.MakeCollection(ctx, temporarySchemaProvider, nil /* monitor */)
	return &c
}
