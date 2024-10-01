// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

// TestingKnobs are the testing knobs for flowinfra.
type TestingKnobs struct {
	// FlowRegistryDraining overrides draining state of the registry.
	FlowRegistryDraining func() bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
