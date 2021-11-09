// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scrun

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"

// NewSchemaChangerTestingKnobs are testing knobs for the executor.
type NewSchemaChangerTestingKnobs struct {
	// BeforeStage is called before ops passed to the executor are executed.
	// Errors returned are injected into the executor.
	BeforeStage func(ops scplan.Plan, stageIdx int) error

	// BeforeWaitingForConcurrentSchemaChanges is called at the start of waiting
	// for concurrent schema changes to finish.
	BeforeWaitingForConcurrentSchemaChanges func(stmts []string)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*NewSchemaChangerTestingKnobs) ModuleTestingKnobs() {}
