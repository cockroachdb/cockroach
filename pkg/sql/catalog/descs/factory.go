// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydrateddesccache"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// CollectionFactory is used to construct a new Collection.
type CollectionFactory struct {
	settings                             *cluster.Settings
	codec                                keys.SQLCodec
	leaseMgr                             *lease.Manager
	virtualSchemas                       VirtualCatalogHolder
	hydrated                             *hydrateddesccache.Cache
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

type Txn interface {
	isql.Txn
	Descriptors() *Collection
	Regions() RegionProvider
}

// DB is used to enable running multiple queries with an internal
// executor in a transactional manner.
type DB interface {
	isql.DB

	// DescsTxn is similar to DescsTxnWithExecutor but without an internal executor.
	// It creates a descriptor collection that lives within the scope of the given
	// function, and is a convenient method for running a transaction on
	// them.
	DescsTxn(
		ctx context.Context,
		f func(context.Context, Txn) error,
		opts ...isql.TxnOption,
	) error
}

// NewCollectionFactory constructs a new CollectionFactory which holds onto
// the node-level dependencies needed to construct a Collection.
func NewCollectionFactory(
	ctx context.Context,
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	virtualSchemas VirtualCatalogHolder,
	hydrated *hydrateddesccache.Cache,
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
		defaultMonitor: mon.NewUnlimitedMonitor(ctx, mon.Options{
			Name:     mon.MakeMonitorName("CollectionFactoryDefaultUnlimitedMonitor"),
			Settings: settings,
		}),
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
	// If the leaseMgr  is nil then ensure we have a nil LeaseManager interface,
	// otherwise comparisons against a nil implementation will fail.
	var lm LeaseManager
	lm = cf.leaseMgr
	if cf.leaseMgr == nil {
		lm = nil
	}
	return &Collection{
		settings:                cf.settings,
		version:                 v,
		hydrated:                cf.hydrated,
		virtual:                 makeVirtualDescriptors(cf.virtualSchemas),
		leased:                  makeLeasedDescriptors(lm),
		uncommitted:             makeUncommittedDescriptors(cfg.monitor),
		uncommittedComments:     makeUncommittedComments(),
		uncommittedZoneConfigs:  makeUncommittedZoneConfigs(),
		cr:                      catkv.NewCatalogReader(cf.codec, v, cf.systemDatabase, cfg.monitor),
		temporarySchemaProvider: cfg.dsdp,
		validationModeProvider:  cfg.dsdp,
	}
}

// RegionProvider abstracts the lookup of regions. It is used to implement
// crdb_internal.regions, which ultimately drives `SHOW REGIONS` and the
// logic in the commands to manipulate multi-region features.
type RegionProvider interface {
	// GetRegions provides access to the set of regions available to the
	// current tenant.
	GetRegions(ctx context.Context) (*serverpb.RegionsResponse, error)

	// SynthesizeRegionConfig returns a RegionConfig that describes the
	// multiregion setup for the given database ID.
	SynthesizeRegionConfig(
		ctx context.Context,
		dbID descpb.ID,
		opts ...multiregion.SynthesizeRegionConfigOption,
	) (multiregion.RegionConfig, error)
}
