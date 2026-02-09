// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// WithDiskStallController creates a ConfigOption that wraps the engine's VFS
// with a DiskStallController, enabling in-process disk stall simulation for
// unit tests.
//
// The controller pointer is set to the created controller, allowing the test
// to control stall behavior after the engine is opened.
//
// Example usage:
//
//	var controller *fs.DiskStallController
//	env := storage.InitEnv(ctx, vfs.NewMem(), dir, ...)
//	db, err := storage.Open(ctx, env, settings,
//	    storage.WithDiskStallController(&controller, nil),
//	)
//
//	// Later in test:
//	controller.Block()           // Stall all I/O
//	controller.WaitForBlocked(1) // Wait for operation to block
//	controller.Release()         // Resume I/O
//
// The opFilter parameter, if non-nil, determines which operations are subject
// to stalling. Common filters are available in the fs package:
//   - fs.StallAllOps: stall all operations
//   - fs.StallWriteOps: stall only write operations
//   - fs.StallReadOps: stall only read operations
//   - fs.StallSyncOps: stall only sync operations
//
// If opFilter is nil, all operations are affected.
func WithDiskStallController(
	controllerOut **fs.DiskStallController, opFilter func(errorfs.Op) bool,
) ConfigOption {
	return func(cfg *engineConfig) error {
		// We must wrap cfg.env's internal defaultFS rather than cfg.opts.FS,
		// because newPebble() later sets cfg.opts.FS = cfg.env, which would
		// overwrite any wrapping we do on cfg.opts.FS. By wrapping the env's
		// internal defaultFS, the env itself delegates to our wrapped FS.
		injector := fs.NewStallableInjector(opFilter)
		cfg.env.WrapFS(func(innerFS vfs.FS) vfs.FS {
			return errorfs.Wrap(innerFS, injector)
		})
		*controllerOut = fs.NewDiskStallControllerFromInjector(injector, cfg.env.DefaultFS())
		return nil
	}
}

// WithDiskStallInjector creates a ConfigOption that wraps the engine's VFS
// with a StallableInjector directly, for cases where you need more fine-grained
// control than DiskStallController provides.
//
// Example usage:
//
//	var injector *fs.StallableInjector
//	env := storage.InitEnv(ctx, vfs.NewMem(), dir, ...)
//	db, err := storage.Open(ctx, env, settings,
//	    storage.WithDiskStallInjector(&injector, fs.StallWriteOps),
//	)
//
//	// Later in test:
//	injector.SetBlock()
//	injector.Enable()
func WithDiskStallInjector(
	injectorOut **fs.StallableInjector, opFilter func(errorfs.Op) bool,
) ConfigOption {
	return func(cfg *engineConfig) error {
		// We must wrap cfg.env's internal defaultFS rather than cfg.opts.FS,
		// because newPebble() later sets cfg.opts.FS = cfg.env, which would
		// overwrite any wrapping we do on cfg.opts.FS.
		injector := fs.NewStallableInjector(opFilter)
		cfg.env.WrapFS(func(innerFS vfs.FS) vfs.FS {
			return errorfs.Wrap(innerFS, injector)
		})
		*injectorOut = injector
		return nil
	}
}
