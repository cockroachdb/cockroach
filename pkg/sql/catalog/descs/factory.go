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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// CollectionFactory is used to construct a new Collection.
type CollectionFactory struct {
	settings                             *cluster.Settings
	codec                                keys.SQLCodec
	leaseMgr                             *lease.Manager
	virtualSchemas                       catalog.VirtualSchemas
	hydrated                             *hydrateddesc.Cache
	systemDatabase                       *catkv.SystemDatabaseCache
	spanConfigSplitter                   spanconfig.Splitter
	spanConfigLimiter                    spanconfig.Limiter
	defaultMonitor                       *mon.BytesMonitor
	defaultDescriptorSessionDataProvider DescriptorSessionDataProvider
}

// GetClusterSettings returns the cluster setting from the collection factory.
func (cf *CollectionFactory) GetClusterSettings() *cluster.Settings {
	return cf.settings
}

// TxnManager is used to enable running multiple queries with an internal
// executor in a transactional manner.
type TxnManager interface {
	sqlutil.InternalExecutorFactory

	// DescsTxnWithExecutor enables using an internal executor to run sql
	// statements in a transactional manner. It creates a descriptor collection
	// that lives within the scope of the passed in TxnWithExecutorFunc, and
	// also ensures that the internal executor also share the same descriptor
	// collection. Please use this interface if you want to run multiple sql
	// statement with an internal executor in a txn.
	DescsTxnWithExecutor(
		ctx context.Context,
		db *kv.DB,
		sd *sessiondata.SessionData,
		f TxnWithExecutorFunc,
		opts ...sqlutil.TxnOption,
	) error

	// DescsTxn is similar to DescsTxnWithExecutor but without an internal executor.
	// It creates a descriptor collection that lives within the scope of the given
	// function, and is a convenient method for running a transaction on
	// them.
	DescsTxn(
		ctx context.Context,
		db *kv.DB,
		f func(context.Context, *kv.Txn, *Collection) error,
		opts ...sqlutil.TxnOption,
	) error
}

// InternalExecutorCommitTxnFunc is to commit the txn associated with an
// internal executor.
type InternalExecutorCommitTxnFunc func(ctx context.Context) error

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
	defaultDescriptorSessionDataProvider DescriptorSessionDataProvider,
) *CollectionFactory {
	return &CollectionFactory{
		settings:           settings,
		codec:              leaseMgr.Codec(),
		leaseMgr:           leaseMgr,
		virtualSchemas:     virtualSchemas,
		hydrated:           hydrated,
		systemDatabase:     leaseMgr.SystemDatabaseCache(),
		spanConfigSplitter: spanConfigSplitter,
		spanConfigLimiter:  spanConfigLimiter,
		defaultMonitor: mon.NewUnlimitedMonitor(ctx, "CollectionFactoryDefaultUnlimitedMonitor",
			mon.MemoryResource, nil /* curCount */, nil, /* maxHist */
			0 /* noteworthy */, settings),
		defaultDescriptorSessionDataProvider: defaultDescriptorSessionDataProvider,
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

type constructorConfig struct {
	dsdp    DescriptorSessionDataProvider
	monitor *mon.BytesMonitor
}

// Option is how optional construction parameters are provided to the
// CollectionFactory construction method.
type Option func(b *constructorConfig)

// WithDescriptorSessionDataProvider supplies a DescriptorSessionDataProvider
// instance to the Collection constructor.
func WithDescriptorSessionDataProvider(
	dsdp DescriptorSessionDataProvider,
) func(cfg *constructorConfig) {
	return func(cfg *constructorConfig) {
		cfg.dsdp = dsdp
	}
}

// WithMonitor supplies a mon.BytesMonitor instance to the Collection
// constructor.
func WithMonitor(monitor *mon.BytesMonitor) func(b *constructorConfig) {
	return func(cfg *constructorConfig) {
		cfg.monitor = monitor
	}
}

// NewCollection constructs a new Collection.
// When no DescriptorSessionDataProvider is provided, the factory falls back to
// the default instances which behaves as if the session data stack were empty.
// Whe no mon.BytesMonitor is provided, the factory falls back to a default,
// unlimited monitor for which all downstream resource allocation/releases are
// no-ops.
func (cf *CollectionFactory) NewCollection(ctx context.Context, options ...Option) *Collection {
	cfg := constructorConfig{
		dsdp:    cf.defaultDescriptorSessionDataProvider,
		monitor: cf.defaultMonitor,
	}
	for _, opt := range options {
		opt(&cfg)
	}
	v := cf.settings.Version.ActiveVersion(ctx)
	return &Collection{
		settings:                cf.settings,
		version:                 v,
		hydrated:                cf.hydrated,
		virtual:                 makeVirtualDescriptors(cf.virtualSchemas),
		leased:                  makeLeasedDescriptors(cf.leaseMgr),
		uncommitted:             makeUncommittedDescriptors(cfg.monitor),
		uncommittedComments:     makeUncommittedComments(),
		uncommittedZoneConfigs:  makeUncommittedZoneConfigs(),
		cr:                      catkv.NewCatalogReader(cf.codec, v, cf.systemDatabase, cfg.monitor),
		temporarySchemaProvider: cfg.dsdp,
		validationModeProvider:  cfg.dsdp,
	}
}
