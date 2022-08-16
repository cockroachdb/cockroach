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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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
	ieFactoryWithTxn   InternalExecutorFactoryWithTxn
}

// InternalExecutorFactoryWithTxn is used to create an internal executor
// with associated extra txn state information.
// It should only be used as a field hanging off CollectionFactory.
type InternalExecutorFactoryWithTxn interface {
	NewInternalExecutorWithTxn(
		sd *sessiondata.SessionData,
		sv *settings.Values,
		txn *kv.Txn,
		descCol *Collection,
	) (sqlutil.InternalExecutor, sqlutil.InternalExecutorCommitTxnFunc)
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

// NewCollection constructs a new Collection.
func (cf *CollectionFactory) NewCollection(
	ctx context.Context, temporarySchemaProvider TemporarySchemaProvider, monitor *mon.BytesMonitor,
) *Collection {
	if monitor == nil {
		// If an upstream monitor is not provided, the default, unlimited monitor will be used.
		// All downstream resource allocation/releases on this default monitor will then be no-ops.
		monitor = cf.defaultMonitor
	}
	return newCollection(ctx, cf.leaseMgr, cf.settings, cf.codec, cf.hydrated, cf.systemDatabase,
		cf.virtualSchemas, temporarySchemaProvider, monitor)
}

// SetInternalExecutorWithTxn is to set the internal executor factory hanging
// off the collection factory.
func (cf *CollectionFactory) SetInternalExecutorWithTxn(
	ieFactoryWithTxn InternalExecutorFactoryWithTxn,
) {
	cf.ieFactoryWithTxn = ieFactoryWithTxn
}
