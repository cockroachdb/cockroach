// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterSequenceNode struct {
	n       *tree.AlterSequence
	seqDesc *tabledesc.Mutable
}

// AlterSequence transforms a tree.AlterSequence into a plan node.
func (p *planner) AlterSequence(ctx context.Context, n *tree.AlterSequence) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER SEQUENCE",
	); err != nil {
		return nil, err
	}

	_, seqDesc, err := p.ResolveMutableTableDescriptorEx(
		ctx, n.Name, !n.IfExists, tree.ResolveRequireSequenceDesc,
	)
	if err != nil {
		return nil, err
	}
	if seqDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, seqDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &alterSequenceNode{n: n, seqDesc: seqDesc}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because ALTER SEQUENCE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterSequenceNode) ReadingOwnWrites() {}

// GetLastSequenceValue get the last valid sequence value when unavailable values are caused by increment
func GetLastSequenceValue(lastVal int64, oldIncrement int64, oldBound int64) (int64, error) {
	if (lastVal > oldBound && oldIncrement > 0) || (lastVal < oldBound && oldIncrement < 0) {
		lastValidVal := lastVal
		figureBeyond := (lastVal - oldBound) % oldIncrement
		// oldBound is the last valid sequence value
		if figureBeyond == 0 {
			lastValidVal = oldBound
		} else {
			lastValidVal = oldBound - (oldIncrement - figureBeyond)
		}
		return lastValidVal, nil
	}
	return 0, pgerror.Newf(
		pgcode.SequenceGeneratorLimitExceeded,
		"Select currval please, it's abnormal")
}

func (n *alterSequenceNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("sequence"))
	desc := n.seqDesc

	oldMinValue := desc.SequenceOpts.MinValue
	oldMaxValue := desc.SequenceOpts.MaxValue
	oldIncrement := desc.SequenceOpts.Increment
	oldStart := desc.SequenceOpts.Start
	// isAlterAfterInit : true incidate alter_sequence_stmt after create_sequence_stmt without use
	isAlterWithOutUse := false

	existingType := types.Int
	if desc.GetSequenceOpts().AsIntegerType != "" {
		switch desc.GetSequenceOpts().AsIntegerType {
		case types.Int2.SQLString():
			existingType = types.Int2
		case types.Int4.SQLString():
			existingType = types.Int4
		case types.Int.SQLString():
			// Already set.
		default:
			return errors.AssertionFailedf("sequence has unexpected type %s", desc.GetSequenceOpts().AsIntegerType)
		}
	}
	if err := assignSequenceOptions(
		params.ctx,
		params.p,
		desc.SequenceOpts,
		n.n.Options,
		false, /* setDefaults */
		desc.GetID(),
		desc.ParentID,
		existingType,
	); err != nil {
		return err
	}
	opts := desc.SequenceOpts
	seqValueKey := params.p.ExecCfg().Codec.SequenceKey(uint32(desc.ID))

	getSequenceValue := func() (int64, error) {
		kv, err := params.p.txn.Get(params.ctx, seqValueKey)
		if err != nil {
			return 0, err
		}
		return kv.ValueInt(), nil
	}

	// Due to the semantics of sequence caching (see sql.planner.incrementSequenceUsingCache()),
	// it is possible for a sequence to have a value that exceeds its MinValue or MaxValue. Users
	// do no see values extending the sequence's bounds, and instead see "bounds exceeded" errors.
	// To make a usable again after exceeding its bounds, there are two options:
	// 1. The user changes the sequence's value by calling setval(...)
	// 2. The user performs a schema change to alter the sequences MinValue or MaxValue. In this case, the
	// value of the sequence must be restored to the original MinValue or MaxValue transactionally.
	// The code below handles the second case.

	sequenceVal, err := getSequenceValue()
	if err != nil {
		return err
	}

	// If the sequenceVal exceeded the old MinValue or MaxValue, we must set sequenceVal to the last valid one.
	// 1. Values is just init, when the sequence is altered before it has ever been used
	// In this case, last valid sequenceVal is oldStart.
	// Set the initial value to the oldStartValue(just like pg), when last valid sequenceVal in [newMinValue, newMaxValue]
	if sequenceVal == oldStart-oldIncrement {
		isAlterWithOutUse = true
		if oldStart > opts.MaxValue {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"START value (%d) cannot be greater than MAXVALUE (%d)", oldStart, opts.MaxValue)
		}
		if oldStart < opts.MinValue {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"START value (%d) cannot be less than MINVALUE (%d)", oldStart, opts.MinValue)
		}
		sequenceVal = oldStart - opts.Increment
	} else if (sequenceVal < oldMinValue && oldIncrement < 0) || (sequenceVal > oldMaxValue && oldIncrement > 0) {
		// 2. The sequenceVal out of range result from increment
		var oldBound int64
		if sequenceVal < oldMinValue && oldIncrement < 0 {
			// out of oldMinValue when decrease
			oldBound = oldMinValue
		} else if sequenceVal > oldMaxValue && oldIncrement > 0 {
			// out of oldMaxValue when increase
			oldBound = oldMaxValue
		}
		sequenceVal, err = GetLastSequenceValue(sequenceVal, oldIncrement, oldBound)
		if err != nil {
			return err
		}
	}
	if err := params.p.txn.Put(params.ctx, seqValueKey, sequenceVal); err != nil {
		return err
	}
	// When isAlterWithOutUse is true, allow (opts.Start - opts.Increment) exceed the bounds.
	// The opts.Start must be in [newMinValue, newMaxValue]
	if isAlterWithOutUse == false {
		if sequenceVal > opts.MaxValue {
			return pgerror.Newf(
				pgcode.SequenceGeneratorLimitExceeded,
				"MAXVALUE cannot be made to be less than the current value")
		} else if sequenceVal < opts.MinValue {
			return pgerror.Newf(
				pgcode.SequenceGeneratorLimitExceeded,
				"MINVALUE cannot be made to be exceed than the current value")
		}
	}
	// Clear out the cache and update the last value.
	params.p.sessionDataMutatorIterator.applyOnEachMutator(func(m sessionDataMutator) {
		m.initSequenceCache()
		m.RecordLatestSequenceVal(uint32(n.seqDesc.GetID()), sequenceVal)
	})

	if err := params.p.writeSchemaChange(
		params.ctx, n.seqDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Record this sequence alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return params.p.logEvent(params.ctx,
		n.seqDesc.ID,
		&eventpb.AlterSequence{
			SequenceName: params.p.ResolvedName(n.n.Name).FQString(),
		})
}

func (n *alterSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterSequenceNode) Close(context.Context)        {}
