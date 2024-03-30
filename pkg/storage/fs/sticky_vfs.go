// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fs

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble/vfs"
)

// StickyOption is a config option for a sticky vfs
// registry that can be passed to NewStickyRegistry.
type StickyOption func(cfg *stickyConfig)

type stickyConfig struct {
	newFS func() *vfs.MemFS // by default vfs.NewMem
}

// UseStrictMemFS option instructs StickyRegistry to produce strict in-memory
// filesystems, i.e. to use vfs.NewStrictMem instead of vfs.NewMem.
var UseStrictMemFS = StickyOption(func(cfg *stickyConfig) {
	cfg.newFS = vfs.NewStrictMem
})

// StickyRegistry manages the lifecycle of sticky in-memory filesystems. It
// is intended for use in demos and/or tests, where we want in-memory storage
// nodes to persist between killed nodes.
type StickyRegistry interface {
	// Get returns the named in-memory FS, constructing a new one if this is the
	// first time a FS with the provided ID has been requested.
	Get(stickyVFSID string) *vfs.MemFS
}

// stickyRegistryImpl is the bookkeeper for all active sticky filesystems,
// keyed by their id. It implements the StickyRegistry interface.
type stickyRegistryImpl struct {
	entries map[string]*vfs.MemFS
	mu      syncutil.Mutex
	cfg     stickyConfig
}

// NewStickyRegistry creates a new StickyRegistry.
func NewStickyRegistry(opts ...StickyOption) StickyRegistry {
	registry := &stickyRegistryImpl{
		entries: map[string]*vfs.MemFS{},
	}
	for _, opt := range opts {
		opt(&registry.cfg)
	}
	// Use the regular in-memory filesystem, unless specified otherwise.
	if registry.cfg.newFS == nil {
		registry.cfg.newFS = vfs.NewMem
	}
	return registry
}

// Get implements the StickyRegistry interface.
func (registry *stickyRegistryImpl) Get(stickyVFSID string) *vfs.MemFS {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if fs, ok := registry.entries[stickyVFSID]; ok {
		return fs
	}
	fs := registry.cfg.newFS()
	registry.entries[stickyVFSID] = fs
	return fs
}
