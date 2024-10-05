// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble/vfs"
)

// StickyVFSOption is a config option for a sticky engine
// registry that can be passed to NewStickyVFSRegistry.
type StickyVFSOption func(cfg *stickyConfig)

type stickyConfig struct {
	newFS func() *vfs.MemFS // by default vfs.NewMem
}

// UseStrictMemFS option instructs StickyVFSRegistry to produce strict in-memory
// filesystems, i.e. to use vfs.NewStrictMem instead of vfs.NewMem.
var UseStrictMemFS = StickyVFSOption(func(cfg *stickyConfig) {
	cfg.newFS = vfs.NewStrictMem
})

// StickyVFSRegistry manages the lifecycle of sticky in-memory filesystems. It
// is intended for use in demos and/or tests, where we want in-memory storage
// nodes to persist between killed nodes.
type StickyVFSRegistry interface {
	// Get returns the named in-memory FS, constructing a new one if this is the
	// first time a FS with the provided ID has been requested.
	Get(stickyVFSID string) *vfs.MemFS
}

// stickyVFSRegistryImpl is the bookkeeper for all active sticky filesystems,
// keyed by their id. It implements the StickyVFSRegistry interface.
type stickyVFSRegistryImpl struct {
	entries map[string]*vfs.MemFS
	mu      syncutil.Mutex
	cfg     stickyConfig
}

// NewStickyVFSRegistry creates a new StickyVFSRegistry.
func NewStickyVFSRegistry(opts ...StickyVFSOption) StickyVFSRegistry {
	registry := &stickyVFSRegistryImpl{
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

// Get implements the StickyVFSRegistry interface.
func (registry *stickyVFSRegistryImpl) Get(stickyVFSID string) *vfs.MemFS {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if fs, ok := registry.entries[stickyVFSID]; ok {
		return fs
	}
	fs := registry.cfg.newFS()
	registry.entries[stickyVFSID] = fs
	return fs
}
