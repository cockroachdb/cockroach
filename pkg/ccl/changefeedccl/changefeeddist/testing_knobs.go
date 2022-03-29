// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeeddist

import "github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"

// TestingKnobs are the testing knobs for changefeeddist.
type TestingKnobs struct {
	// OnDistflowSpec is called when specs for distflow planning have been created
	OnDistflowSpec func(aggregatorSpecs []*execinfrapb.ChangeAggregatorSpec, frontierSpec *execinfrapb.ChangeFrontierSpec)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
