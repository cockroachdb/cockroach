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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble/vfs"
)

// StickyVFSOption is a config option for a sticky engine
// registry that can be passed to NewStickyVFSRegistry.
type StickyVFSOption func(cfg *stickyConfig)

type stickyConfig struct {
	// Preserved for anticipated future options, such as the ability to use
	// Pebble's "strict" MemFS.
}

// StickyVFSRegistry manages the lifecycle of sticky in-memory filesystems. It
// is intended for use in demos and/or tests, where we want in-memory storage
// nodes to persist between killed nodes.
type StickyVFSRegistry interface {
	// Get returns the named in-memory FS, constructing a new one if this is the
	// first time a FS with the provided ID has been requested.
	Get(stickyVFSID string) vfs.FS
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
	return registry
}

// Get implements the StickyVFSRegistry interface.
func (registry *stickyVFSRegistryImpl) Get(stickyVFSID string) vfs.FS {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if fs, ok := registry.entries[stickyVFSID]; ok {
		return fs
	}
	fs := vfs.NewMem()
	registry.entries[stickyVFSID] = fs
	return fs
}
