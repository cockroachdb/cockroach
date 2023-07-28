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

// StickyVFSOption is a config option for a sticky engine
// registry that can be passed to NewStickyVFSRegistry.
type StickyVFSOption func(cfg *stickyConfig)

// ReuseEnginesDeprecated configures a sticky VFS registry to return exactly the
// same Engine without closing and re-opening from the underlying in-memory FS.
// This option is deprecated. Callers should refactor their usage to not depend
// on ephemeral, non-persisted state of an Engine.
var ReuseEnginesDeprecated StickyVFSOption = func(cfg *stickyConfig) {
	cfg.reuseEngines = true
}

type stickyConfig struct {
	// reuseEngines is true if a sticky engine registry should return an existing
	// engine instead of reopening it from the underlying in-memory FS.
	reuseEngines bool
}

// StickyVFSRegistry manages the lifecycle of sticky in-memory filesystems. It
// is intended for use in demos and/or tests, where we want in-memory storage
// nodes to persist between killed nodes.
type StickyVFSRegistry interface {
	// Open returns an engine associated with the given spec's StickyVFSID. If
	// neither Open nor Get has been called with the given spec's StickyVFSID
	// before, Open will create a new in-memory filesystem. Otherwise, the new
	// Engine will re-open the existing store held within the in-memory
	// filesystem.
	//
	// If the registry was created with the ReuseEnginesDeprecated option, Open
	// will return an Engine whose Close method does not close the underlying
	// engine. Subsequent Open calls will return the same Engine. With this
	// option, at most one engine with a given id can be active at any given
	// time. Note that with the ReuseEnginesDeprecated option, an Open call that
	// returns an existing sticky engine will not respect modified configuration
	// or attributes. When using the ReuseEnginesDeprecated option, the caller
	// must call CloseAllEngines when they're finished to close the underlying
	// engines.
	Open(ctx context.Context, cfg *Config, spec base.StoreSpec) (storage.Engine, error)
	// Get returns the named in-memory FS.
	Get(spec base.StoreSpec) (vfs.FS, error)
	// CloseAllEngines closes all open sticky in-memory engines that were
	// created by this registry. Calling this method is required when using the
	// ReuseEnginesDeprecated option.
	CloseAllEngines()
}

// stickyVFSRegistryImpl is the bookkeeper for all active sticky filesystems,
// keyed by their id. It implements the StickyVFSRegistry interface.
type stickyVFSRegistryImpl struct {
	entries map[string]*vfs.MemFS
	engines map[string]*stickyInMemEngine
	mu      syncutil.Mutex
	cfg     stickyConfig
}

// NewStickyVFSRegistry creates a new StickyVFSRegistry.
func NewStickyVFSRegistry(opts ...StickyVFSOption) StickyVFSRegistry {
	registry := &stickyVFSRegistryImpl{
		entries: map[string]*vfs.MemFS{},
		engines: map[string]*stickyInMemEngine{},
	}
	for _, opt := range opts {
		opt(&registry.cfg)
	}
	return registry
}

// Open implements the StickyVFSRegistry interface.
func (registry *stickyVFSRegistryImpl) Open(
	ctx context.Context, cfg *Config, spec base.StoreSpec,
) (storage.Engine, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	// Look up the VFS.
	fs, ok := registry.entries[spec.StickyVFSID]

	// If the registry is configured to reuse whole Engines (eg, skipping
	// shutdown and recovery of the storage engine), then check if we already
	// have an open Engine.
	if ok && registry.cfg.reuseEngines {
		if engine, engineOk := registry.engines[spec.StickyVFSID]; engineOk {
			if !engine.closed {
				return nil, errors.Errorf("sticky engine %s has not been closed", spec.StickyVFSID)
			}
			log.Infof(ctx, "re-using existing sticky in-mem engine %s", spec.StickyVFSID)
			engine.closed = false
			return engine, nil
		}
	}

	// If the VFS doesn't yet exist, construct a new one and save the
	// association.
	if !ok {
		fs = vfs.NewMem()
		registry.entries[spec.StickyVFSID] = fs
	}

	// Create a new engine.
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

	log.Infof(ctx, "creating engine with sticky in-memory VFS %s", spec.StickyVFSID)
	engine := storage.InMemFromFS(ctx, fs, "", cluster.MakeClusterSettings(), options...)

	// If this registry is configured to keep Engines open rather recovering
	// from filesystem state, wrap the Engine within a *stickyInMemEngine
	// wrapper so that calls to Close do not Close the underlying Engine.
	if registry.cfg.reuseEngines {
		wrappedEngine := &stickyInMemEngine{
			id:     spec.StickyVFSID,
			closed: false,
			Engine: engine,
			fs:     fs,
		}
		registry.engines[spec.StickyVFSID] = wrappedEngine
		return wrappedEngine, nil
	}
	return engine, nil
}

func (registry *stickyVFSRegistryImpl) Get(spec base.StoreSpec) (vfs.FS, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if fs, ok := registry.entries[spec.StickyVFSID]; ok {
		return fs, nil
	} else {
		fs = vfs.NewMem()
		registry.entries[spec.StickyVFSID] = fs
		return fs, nil
	}
}

// CloseAllEngines closes all open sticky in-memory engines that were
// created by this registry. Calling this method is required when using the
// ReuseEnginesDeprecated option.
func (registry *stickyVFSRegistryImpl) CloseAllEngines() {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	for id, engine := range registry.engines {
		engine.closed = true
		engine.Engine.Close()
		delete(registry.entries, id)
		delete(registry.engines, id)
	}
}

// stickyInMemEngine extends a normal engine, but holds on to the Engine between
// Opens to allow subsequent Opens to reuse a previous Engine instance. This
// type is used only when a the StickyVFSRegistry is created with the
// ReuseEnginesDeprecated option.
//
// Engine.Close does not close the underlying Engine. The engine is kept open
// until CloseAllEngines is called, hence being "sticky".
type stickyInMemEngine struct {
	// id is the unique identifier for this sticky engine.
	id string
	// closed indicates whether the current engine has been closed.
	closed bool
	// Engine extends the Engine interface.
	storage.Engine
	// Underlying in-mem filesystem backing the engine.
	fs vfs.FS
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
