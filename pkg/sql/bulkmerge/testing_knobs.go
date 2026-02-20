// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
)

// TestingKnobs contains testing hooks for the distributed merge pipeline.
type TestingKnobs struct {
	// RunBeforeMergeTask, if set, is invoked before the merge processor merges
	// a specific task. Returning an error causes the processor to fail the task.
	RunBeforeMergeTask func(
		ctx context.Context,
		flowCtx *execinfra.FlowCtx,
		taskID taskset.TaskID,
		spec execinfrapb.BulkMergeSpec,
	) error

	// InjectDuplicateKey is called for each key during merge. Returning true
	// causes the key to be written twice, injecting a duplicate for testing.
	InjectDuplicateKey func(iteration, maxIteration int32) bool
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs implements base.ModuleTestingKnobs.
func (*TestingKnobs) ModuleTestingKnobs() {}
