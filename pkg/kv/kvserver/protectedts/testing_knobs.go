// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package protectedts

import "github.com/cockroachdb/cockroach/pkg/base"

// TestingKnobs provide fine-grained control over the various span config
// components for testing.
type TestingKnobs struct {

}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
