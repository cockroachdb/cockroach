// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package everynode provides a mechanism for running an idempotent closure on
// each node in the cluster during upgrades.
package everynode

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Node is a minimal subset of server.Node.
type Node interface{}

// Hook WIP
type Hook struct {
	// Name is a unique name for this Hook.
	Name string

	// RunFn is an idempotent function.
	RunFn func(context.Context, Node) error
}

var registry struct {
	mu struct {
		syncutil.Mutex
		hooks map[string]Hook
	}
}

// Register is called in an init func to add a hook implementation for use with
// RunHookOnEveryNode.
func Register(h Hook) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.mu.hooks == nil {
		registry.mu.hooks = make(map[string]Hook)
	}
	if _, ok := registry.mu.hooks[h.Name]; ok {
		panic(errors.AssertionFailedf(`multiple hooks with name: %s`, h.Name))
	}
	registry.mu.hooks[h.Name] = h
}

// RunHooksOnThisNode executes the requested closure on this node.
func RunHooksOnThisNode(ctx context.Context, node Node, names ...string) error {
	for _, name := range names {
		registry.mu.Lock()
		hook, ok := registry.mu.hooks[name]
		registry.mu.Unlock()
		if !ok {
			return errors.Errorf(`could not find hook with name: %s`, name)
		}
		if err := hook.RunFn(ctx, node); err != nil {
			return errors.Wrapf(err, `running hook: %s`, name)
		}
	}
	return nil
}

// RunHookOnEveryNode executes the requested hook closure on each node in the
// cluster and each node added to the cluster in the future. If no error is
// returned, the caller is guaranteed that no node will ever again serve SQL or
// KV traffic without first having run this hook.
//
// This is used during cluster version upgrades and requires that each node in
// the cluster be available. It blocks until each node has been contacted via
// RPC and returned a successful response.
//
// WIP how do we communicate to the user if this is blocking while a node is
// down?
//
// WIP what about nodes that are decommissioned and later re-added? Is this even
// legal or will they get a new node id?
func RunHookOnEveryNode(ctx context.Context, name string) error {
	panic(`WIP`)
	// Edge cases to handle:
	// - A node is added while this is running
}
