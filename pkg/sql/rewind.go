// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/pkg/errors"
)

// isRewindable returns true iff the plan can be re-used for multiple
// executions, by calling startExec/Next/Values multiple times without
// intervening Close and re-planning. If it returns true, the
// startExec() call after the first must have the rewind parameter set
// to true.
//
// To determine this, this function checks the planNodeNotRewindable
// interface predicate on all planNodes in the tree. If any are found,
// the plan is considered not rewindable.
func isRewindable(ctx context.Context, plan planNode) (bool, error) {
	o := planObserver{
		enterNode: func(_ context.Context, _ string, p planNode) (bool, error) {
			if _, ok := p.(planNodeNotRewindable); ok {
				return false, errNotRewindableMarker
			}
			return true, nil
		},
	}
	if err := walkPlan(ctx, plan, o); err != nil {
		if err == errNotRewindableMarker {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// planNodeNotRewindable is to be implemented by those specific
// planNodes that cannot be rewinded and reused for multiple
// execution. The list is defined below.
type planNodeNotRewindable interface {
	cannotRewind()
}

var errNotRewindableMarker = errors.New("not rewindable")

// alterIndexNode is rewindable. Note: holds descriptors.
// alterTableNode is rewindable. Note: holds descriptors.
// alterSequenceNode is rewindable. Note: holds descriptors.

// alterUserSetPasswordNode is rewindable.
// FIXME(knz/jordan): check that the name resolution closure is robust wrt reuses.

// cancelQueryNode is rewindable.
// controlJobNode is rewindable.

func (*copyNode) cannotRewind() {}

// createDatabaseNode is rewindable.

// createIndexNode is rewindable. Note: holds descriptors.
// createTableNode is rewindable. Note: holds descriptors.
// CreateUserNode is rewindable. Note: holds descriptors.
// createViewNode is rewindable. Note: holds descriptors.
// createSequenceNode is rewindable. Note: holds descriptors.
// createStatsNode is rewindable. Note: holds descriptors.

// delayedNode is rewindable.

func (*deleteNode) cannotRewind() {}

// distinctNode is rewindable.

// dropDatabaseNode is rewindable. Note: holds descriptors.

// dropIndexNode is rewindable.

func (*dropTableNode) cannotRewind()    {}
func (*dropViewNode) cannotRewind()     {}
func (*dropSequenceNode) cannotRewind() {}

// DropUserNode is rewindable.
// FIXME(knz/jordan): check that the name resolution closure is robust wrt reuses.

// explainDistSQLNode is rewindable.
// explainPlanNode is rewindable.

// FIXME(knz/jordan): there's something funky about show trace, I don't get it.
func (*showTraceNode) cannotRewind() {}

// filterNode is rewindable.
// groupNode is rewindable.
// unaryNode is rewindable.

func (*hookFnNode) cannotRewind() {}

// indexJoinNode is rewindable.

func (*insertNode) cannotRewind() {}

// joinNode is rewindable.
// limitNode is rewindable.
// ordinalityNode is rewindable.

func (*testingRelocateNode) cannotRewind() {}

// renderNode is rewindable.

// FIXME(knz/jordan) scanNode should really be rewindable.
func (*scanNode) cannotRewind() {}

// scatterNode is rewindable.
// scrubNode is rewindable.

// setVarNode is rewindable.
// setClusterSettingNode is rewindable.
// setZoneConfigNode is rewindable.
// showZoneConfigNode is rewindable.
// showRangesNode is rewindable.

// showFingerprintsNode is rewindable. Note: holds descriptors.

// FIXME(knz/jordan) sortNode should really be rewindable.
func (*sortNode) cannotRewind() {}

// splitNode is rewindable.

// FIXME(knz/jordan) make unionNode rewindable. This may be complicated
// by the wish to close/clear/release the left plan once it has been
// read fully.
func (*unionNode) cannotRewind() {}

func (*updateNode) cannotRewind() {}

// valueGenerator is rewindable.
// valuesNode is rewindable.

// FIXME(knz/jordan) windowNode should probably be rewindable.
func (*windowNode) cannotRewind() {}

// zeroNode is rewindable.
