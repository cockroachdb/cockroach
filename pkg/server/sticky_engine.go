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
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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
	GetOrCreateStickyInMemEngine(
		ctx context.Context, spec base.StoreSpec,
	) (storage.Engine, error)
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

// stickyInMemEnginesRegistryImpl is the bookkeeper for all active
// sticky engines, keyed by their id. It implements the
// StickyInMemEnginesRegistry interface.
type stickyInMemEnginesRegistryImpl struct {
	entries map[string]*stickyInMemEngine
	mu      syncutil.Mutex
}

// NewStickyInMemEnginesRegistry creates a new StickyInMemEnginesRegistry.
func NewStickyInMemEnginesRegistry() StickyInMemEnginesRegistry {
	return &stickyInMemEnginesRegistryImpl{
		entries: map[string]*stickyInMemEngine{},
	}
}

// GetOrCreateStickyInMemEngine implements the StickyInMemEnginesRegistry interface.
func (registry *stickyInMemEnginesRegistryImpl) GetOrCreateStickyInMemEngine(
	ctx context.Context, spec base.StoreSpec,
) (storage.Engine, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if engine, ok := registry.entries[spec.StickyInMemoryEngineID]; ok {
		if !engine.closed {
			return nil, errors.Errorf("sticky engine %s has not been closed", spec.StickyInMemoryEngineID)
		}

		log.Infof(ctx, "re-using sticky in-mem engine %s", spec.StickyInMemoryEngineID)
		engine.closed = false
		return engine, nil
	}

	log.Infof(ctx, "creating new sticky in-mem engine %s", spec.StickyInMemoryEngineID)
	engine := &stickyInMemEngine{
		id:     spec.StickyInMemoryEngineID,
		closed: false,
		Engine: storage.NewInMem(ctx, spec.Attributes, spec.Size.InBytes),
	}
	registry.entries[spec.StickyInMemoryEngineID] = engine
	return engine, nil
}

// CloseAllStickyInMemEngines closes and removes all sticky in memory engines.
func (registry *stickyInMemEnginesRegistryImpl) CloseAllStickyInMemEngines() {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	for _, engine := range registry.entries {
		engine.closed = true
		engine.Engine.Close()
	}

	for id := range registry.entries {
		delete(registry.entries, id)
	}
}
