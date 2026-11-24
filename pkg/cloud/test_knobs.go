// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import "github.com/cockroachdb/cockroach/pkg/util/fault"

type TestingKnobs struct {
	OpFaults fault.Strategy
	IoFaults fault.Strategy
}

func (c *TestingKnobs) ModuleTestingKnobs() {}
