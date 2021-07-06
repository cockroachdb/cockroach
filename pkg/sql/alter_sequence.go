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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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

func (n *alterSequenceNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("sequence"))
	desc := n.seqDesc

	oldMinValue := desc.SequenceOpts.MinValue
	oldMaxValue := desc.SequenceOpts.MaxValue

	err := assignSequenceOptions(
		desc.SequenceOpts, n.n.Options, false /* setDefaults */, &params, desc.GetID(), desc.ParentID,
	)
	if err != nil {
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

	// The sequence is decreasing and the minvalue is being decreased.
	if opts.Increment < 0 && desc.SequenceOpts.MinValue < oldMinValue {
		sequenceVal, err := getSequenceValue()
		if err != nil {
			return err
		}

		// If the sequence exceeded the old MinValue, it must be changed to start at the old MinValue.
		if sequenceVal < oldMinValue {
			err := params.p.txn.Put(params.ctx, seqValueKey, oldMinValue)
			if err != nil {
				return err
			}
		}
	} else if opts.Increment > 0 && desc.SequenceOpts.MaxValue > oldMaxValue {
		sequenceVal, err := getSequenceValue()
		if err != nil {
			return err
		}

		if sequenceVal > oldMaxValue {
			err := params.p.txn.Put(params.ctx, seqValueKey, oldMaxValue)
			if err != nil {
				return err
			}
		}
	}

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
