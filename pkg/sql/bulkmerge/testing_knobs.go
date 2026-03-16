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

	// InjectDuplicateKey is called for each key during merge. Returning a
	// non-nil value causes the current key to be written again with that
	// value, injecting a duplicate for testing. The returned value should
	// differ from the original to simulate a real uniqueness violation;
	// identical values are treated as benign checkpoint-and-resume repeats
	// and silently skipped.
	InjectDuplicateKey func(iteration, maxIteration int32) []byte
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs implements base.ModuleTestingKnobs.
func (*TestingKnobs) ModuleTestingKnobs() {}
