// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import "github.com/cockroachdb/cockroach/pkg/base"

type VecIndexTestingKnobs struct {
	DuringVecIndexPull func()
	BeforeVecIndexWait func()
}

var _ base.ModuleTestingKnobs = (*VecIndexTestingKnobs)(nil)

func (VecIndexTestingKnobs) ModuleTestingKnobs() {}
