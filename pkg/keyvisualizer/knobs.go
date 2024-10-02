// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvisualizer

import "github.com/cockroachdb/cockroach/pkg/base"

type TestingKnobs struct {
	SkipJobBootstrap        bool
	SkipZoneConfigBootstrap bool
}

func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
