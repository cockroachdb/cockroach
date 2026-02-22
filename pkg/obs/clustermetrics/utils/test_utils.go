// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetricutils

type TestingKnobs struct {
	OnRegistrySyncerStart func()
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
