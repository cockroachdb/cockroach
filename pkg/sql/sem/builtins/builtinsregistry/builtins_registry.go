// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package builtinsregistry stores the definitions of builtin functions.
//
// The builtins package imports this package and registers its functions.
// This package exists to avoid import cycles so that catalog packages
// can interact with builtin definitions without needing to import the
// builtins package.
package builtinsregistry

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

var registry = map[string]definition{}

// Subscription is a hook to be called once on all registered builtins.
type Subscription func(name string, props *tree.FunctionProperties, overloads []tree.Overload)

var subscriptions []Subscription

// Register registers a builtin. Intending to be called at init time, it panics
// if a function of the same name has already been registered.
func Register(name string, props *tree.FunctionProperties, overloads []tree.Overload) {
	if _, exists := registry[name]; exists {
		panic("duplicate builtin: " + name)
	}
	for i := range overloads {
		var hook func()
		overloads[i].OnTypeCheck = &hook
	}
	registry[name] = definition{
		props:     props,
		overloads: overloads,
	}
	for _, s := range subscriptions {
		s(name, props, overloads)
	}
}

// AddSubscription defines a hook to be called once on all registered builtins.
// Subscriptions should be added at init() time, but are load-order independent:
// if you add a subscription after a function is registered, it will immediately
// be called on that function, while functions that are registered afterwards will
// also trigger the hook.
func AddSubscription(s Subscription) {
	for name, def := range registry {
		s(name, def.props, def.overloads)
	}
	subscriptions = append(subscriptions, s)
}

// GetBuiltinProperties provides low-level access to a built-in function's properties.
// For a better, semantic-rich interface consider using tree.FunctionDefinition
// instead, and resolve function names via ResolvableFunctionReference.Resolve().
func GetBuiltinProperties(name string) (*tree.FunctionProperties, []tree.Overload) {
	def, ok := registry[name]
	if !ok {
		return nil, nil
	}
	return def.props, def.overloads
}

type definition struct {
	props     *tree.FunctionProperties
	overloads []tree.Overload
}
