// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
)

// TestingKnobs contains testing hooks for the distributed merge pipeline.
type TestingKnobs struct {
	// RunBeforeMergeTask, if set, is invoked before the merge processor merges
	// a specific task. Returning an error causes the processor to fail the task.
	RunBeforeMergeTask func(
		ctx context.Context,
		flowID execinfrapb.FlowID,
		taskID taskset.TaskID,
	) error
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs implements base.ModuleTestingKnobs.
func (*TestingKnobs) ModuleTestingKnobs() {}
