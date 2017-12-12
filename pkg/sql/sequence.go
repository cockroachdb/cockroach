// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"
)

// IncrementSequence implements the tree.EvalPlanner interface.
func (p *planner) IncrementSequence(ctx context.Context, seqName *tree.TableName) (int64, error) {
	if p.session.TxnState.readOnly {
		return 0, readOnlyError("nextval()")
	}
	descriptor, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), seqName)
	if err != nil {
		return 0, err
	}
	seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
	val, err := client.IncrementValRetryable(
		ctx, p.txn.DB(), seqValueKey, descriptor.SequenceOpts.Increment)
	if err != nil {
		return 0, err
	}

	p.session.mu.Lock()
	defer p.session.mu.Unlock()
	p.session.mu.SequenceState.lastSequenceIncremented = descriptor.ID
	p.session.mu.SequenceState.latestValues[descriptor.ID] = val

	return val, nil
}

// GetLastSequenceValue implements the tree.SequenceAccessor interface.
func (p *planner) GetLastSequenceValue(ctx context.Context) (int64, error) {
	p.session.mu.RLock()
	defer p.session.mu.RUnlock()

	seqState := p.session.mu.SequenceState

	if !seqState.nextvalEverCalled() {
		return 0, pgerror.NewError(
			pgerror.CodeObjectNotInPrerequisiteStateError, "lastval is not yet defined in this session")
	}

	return seqState.latestValues[seqState.lastSequenceIncremented], nil
}

// GetLatestValueInSessionForSequence implements the tree.EvalPlanner interface.
func (p *planner) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	descriptor, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), seqName)
	if err != nil {
		return 0, err
	}

	p.session.mu.RLock()
	defer p.session.mu.RUnlock()

	val, ok := p.session.mu.SequenceState.latestValues[descriptor.ID]
	if !ok {
		return 0, pgerror.NewErrorf(
			pgerror.CodeObjectNotInPrerequisiteStateError,
			`currval of sequence "%s" is not yet defined in this session`, seqName)
	}

	return val, nil
}

// SetSequenceValue implements the tree.EvalPlanner interface.
func (p *planner) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64,
) error {
	if p.session.TxnState.readOnly {
		return readOnlyError("setval()")
	}
	descriptor, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), seqName)
	if err != nil {
		return err
	}
	opts := descriptor.SequenceOpts
	if newVal > opts.MaxValue || newVal < opts.MinValue {
		return pgerror.NewErrorf(
			pgerror.CodeNumericValueOutOfRangeError,
			`value %d is out of bounds for sequence "%s" (%d..%d)`,
			newVal, descriptor.Name, opts.MinValue, opts.MaxValue,
		)
	}
	seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
	// TODO(vilterp): not supposed to use Put here
	return p.txn.Put(ctx, seqValueKey, newVal)
}

// sequenceState stores session-scoped state used by sequence builtins.
type sequenceState struct {
	// latestValues stores the last value obtained by nextval() in this session by descriptor id.
	latestValues map[sqlbase.ID]int64

	// lastSequenceIncremented records the descriptor id of the last sequence nextval() was
	// called on in this session.
	lastSequenceIncremented sqlbase.ID
}

func newSequenceState() sequenceState {
	return sequenceState{
		latestValues: make(map[sqlbase.ID]int64),
	}
}

func (ss *sequenceState) nextvalEverCalled() bool {
	return len(ss.latestValues) > 0
}

func readOnlyError(s string) error {
	return pgerror.NewErrorf(pgerror.CodeReadOnlySQLTransactionError,
		"cannot execute %s in a read-only transaction", s)
}
