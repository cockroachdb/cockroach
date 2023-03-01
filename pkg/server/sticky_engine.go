// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// stickyInMemEngine extends a normal engine, but does not allow them to be
// closed using the normal Close() method, instead keeping the engines in
// memory until CloseAllStickyInMemEngines is called, hence being "sticky".
// This prevents users of the in memory engine from having to special
// case "sticky" engines on every instance of "Close".
// It is intended for use in demos and/or tests, where we want in-memory
// storage nodes to persist between killed nodes.
type stickyInMemEngine struct {
	// id is the unique identifier for this sticky engine.
	id string
	// closed indicates whether the current engine has been closed.
	closed bool

	// Engine extends the Engine interface.
	storage.Engine

	// Underlying in-mem filesystem backing the engine
	fs vfs.FS
}

// StickyEngineRegistryConfigOption is a config option for a sticky engine
// registry that can be passed to NewStickyInMemEnginesRegistry.
type StickyEngineRegistryConfigOption func(cfg *stickyEngineRegistryConfig)

// ReplaceEngines configures a sticky engine registry to return a new engine
// with the same underlying in-memory FS instead of simply reopening it in
// the case where it already exists.
var ReplaceEngines StickyEngineRegistryConfigOption = func(cfg *stickyEngineRegistryConfig) {
	cfg.replaceEngines = true
}

// StickyInMemEnginesRegistry manages the lifecycle of sticky engines.
type StickyInMemEnginesRegistry interface {
	// GetOrCreateStickyInMemEngine returns an engine associated with the given id.
	// It will create a new in-memory engine if one does not already exist.
	// At most one engine with a given id can be active in
	// "GetOrCreateStickyInMemEngine" at any given time.
	// Note that if you re-create an existing sticky engine the new attributes
	// and cache size will be ignored.
	// One must Close() on the sticky engine before another can be fetched.
	GetOrCreateStickyInMemEngine(ctx context.Context, cfg *Config, spec base.StoreSpec) (storage.Engine, error)
	// GetUnderlyingFS returns FS backing in mem engine. If engine was not created
	// error is returned.
	GetUnderlyingFS(spec base.StoreSpec) (vfs.FS, error)
	// CloseAllStickyInMemEngines closes all sticky in memory engines that were
	// created by this registry.
	CloseAllStickyInMemEngines()
}

// stickyInMemEngine implements Engine.
var _ storage.Engine = &stickyInMemEngine{}

// Close overwrites the default Engine interface to not close the underlying
// engine if called. We mark the state as closed to reflect a correct result
// in Closed().
func (e *stickyInMemEngine) Close() {
	e.closed = true
}

// Closed overwrites the default Engine interface.
func (e *stickyInMemEngine) Closed() bool {
	return e.closed
}

// SetStoreID implements the StoreIDSetter interface.
func (e *stickyInMemEngine) SetStoreID(ctx context.Context, storeID int32) error {
	return e.Engine.SetStoreID(ctx, storeID)
}

// stickyInMemEnginesRegistryImpl is the bookkeeper for all active
// sticky engines, keyed by their id. It implements the
// StickyInMemEnginesRegistry interface.
type stickyInMemEnginesRegistryImpl struct {
	entries map[string]*stickyInMemEngine
	mu      syncutil.Mutex
	cfg     stickyEngineRegistryConfig
}

// NewStickyInMemEnginesRegistry creates a new StickyInMemEnginesRegistry.
func NewStickyInMemEnginesRegistry(
	opts ...StickyEngineRegistryConfigOption,
) StickyInMemEnginesRegistry {
	var cfg stickyEngineRegistryConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return &stickyInMemEnginesRegistryImpl{
		entries: map[string]*stickyInMemEngine{},
		cfg:     cfg,
	}
}

// GetOrCreateStickyInMemEngine implements the StickyInMemEnginesRegistry interface.
func (registry *stickyInMemEnginesRegistryImpl) GetOrCreateStickyInMemEngine(
	ctx context.Context, cfg *Config, spec base.StoreSpec,
) (storage.Engine, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	var fs vfs.FS
	if engine, ok := registry.entries[spec.StickyInMemoryEngineID]; ok {
		if !engine.closed {
			return nil, errors.Errorf("sticky engine %s has not been closed", spec.StickyInMemoryEngineID)
		}
		if !registry.cfg.replaceEngines {
			log.Infof(ctx, "re-using sticky in-mem engine %s", spec.StickyInMemoryEngineID)
			engine.closed = false
			return engine, nil
		}
		fs = engine.fs
		registry.deleteEngine(spec.StickyInMemoryEngineID)
	} else {
		fs = vfs.NewMem()
	}
	options := []storage.ConfigOption{
		storage.Attributes(spec.Attributes),
		storage.CacheSize(cfg.CacheSize),
		storage.MaxSize(spec.Size.InBytes),
		storage.EncryptionAtRest(spec.EncryptionOptions),
		storage.ForStickyEngineTesting,
	}

	if s := cfg.TestingKnobs.Store; s != nil {
		stk := s.(*kvserver.StoreTestingKnobs)
		if stk.SmallEngineBlocks {
			options = append(options, storage.BlockSize(1))
		}
		if len(stk.EngineKnobs) > 0 {
			options = append(options, stk.EngineKnobs...)
		}
	}

	log.Infof(ctx, "creating new sticky in-mem engine %s", spec.StickyInMemoryEngineID)
	engine := storage.InMemFromFS(ctx, fs, "", cluster.MakeClusterSettings(), options...)

	engineEntry := &stickyInMemEngine{
		id:     spec.StickyInMemoryEngineID,
		closed: false,
		Engine: engine,
		fs:     fs,
	}
	registry.entries[spec.StickyInMemoryEngineID] = engineEntry
	return engineEntry, nil
}

func (registry *stickyInMemEnginesRegistryImpl) GetUnderlyingFS(
	spec base.StoreSpec,
) (vfs.FS, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if engine, ok := registry.entries[spec.StickyInMemoryEngineID]; ok {
		return engine.fs, nil
	}
	return nil, errors.Errorf("engine '%s' was not created", spec.StickyInMemoryEngineID)
}

// CloseAllStickyInMemEngines closes and removes all sticky in memory engines.
func (registry *stickyInMemEnginesRegistryImpl) CloseAllStickyInMemEngines() {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	for id := range registry.entries {
		registry.deleteEngine(id)
	}
}

func (registry *stickyInMemEnginesRegistryImpl) deleteEngine(id string) {
	engine, ok := registry.entries[id]
	if !ok {
		return
	}
	engine.closed = true
	engine.Engine.Close()
	delete(registry.entries, id)
}

type stickyEngineRegistryConfig struct {
	// replaceEngines is true if a sticky engine registry should return a new
	// engine with the same underlying in-memory FS instead of simply reopening
	// it in the case where it already exists.
	replaceEngines bool
}
