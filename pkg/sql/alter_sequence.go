// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterSequenceNode struct {
	zeroInputPlanNode
	nonReusablePlanNode
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

	// Alter sequence with specified options.
	// Does not override existing values if not specified.
	if err := alterSequenceImpl(params, n.seqDesc, n.n.Options, n.n); err != nil {
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

// alterSequenceImpl applies given tree.SequenceOptions to the specified sequence descriptor.
// Existing sequence options are not overridden with default values.
func alterSequenceImpl(
	params runParams,
	seqDesc *tabledesc.Mutable,
	seqOptions tree.SequenceOptions,
	formatter tree.NodeFormatter,
) error {
	oldMinValue := seqDesc.SequenceOpts.MinValue
	oldMaxValue := seqDesc.SequenceOpts.MaxValue
	oldIncrement := seqDesc.SequenceOpts.Increment
	oldStart := seqDesc.SequenceOpts.Start

	existingType := types.Int
	if seqDesc.GetSequenceOpts().AsIntegerType != "" {
		switch seqDesc.GetSequenceOpts().AsIntegerType {
		case types.Int2.SQLString():
			existingType = types.Int2
		case types.Int4.SQLString():
			existingType = types.Int4
		case types.Int.SQLString():
			// Already set.
		default:
			return errors.AssertionFailedf("sequence has unexpected type %s", seqDesc.GetSequenceOpts().AsIntegerType)
		}
	}
	if err := assignSequenceOptions(
		params.ctx,
		params.p,
		seqDesc.SequenceOpts,
		seqOptions,
		false, /* setDefaults */
		seqDesc.GetID(),
		seqDesc.ParentID,
		existingType,
	); err != nil {
		return err
	}
	opts := seqDesc.SequenceOpts
	seqValueKey := params.p.ExecCfg().Codec.SequenceKey(uint32(seqDesc.ID))

	getSequenceValue := func() (int64, error) {
		kv, err := params.p.txn.Get(params.ctx, seqValueKey)
		if err != nil {
			return 0, err
		}

		return kv.ValueInt(), nil
	}

	setSequenceVal := func(val int64) error {
		// Setting the sequence value should always cause the operation to run
		// in the current transaction. This is achieved by treating the sequence
		// as if it were just created.
		if err := params.p.createdSequences.addCreatedSequence(seqDesc.ID); err != nil {
			return err
		}

		err := params.p.txn.Put(params.ctx, seqValueKey, val)
		if err != nil {
			return err
		}

		return nil
	}

	// Due to the semantics of sequence caching (see sql.planner.incrementSequenceUsingCache()), it
	// is possible for a sequence to have a value that exceeds its MinValue or MaxValue. Users do
	// not see values beyond the sequence's bounds, and instead see "bounds exceeded" errors. To
	// make a sequence usable again after exceeding its bounds, there are two options:
	//
	// 1. The user changes the sequence's value by calling setval(...)
	//
	// 2. The user performs a schema change to alter the sequence's MinValue, MaxValue, or
	// Increment. In this case, the value of the sequence must be (transactionally) restored to a
	// value within MinValue and MaxValue.
	//
	// The code below handles the second case.

	if opts.Increment < 0 && (oldIncrement != opts.Increment || opts.MinValue < oldMinValue) {
		// Only get the sequence value from KV if it's needed.
		sequenceVal, err := getSequenceValue()
		if err != nil {
			return err
		}

		// If the sequence were never advanced, its current value is offset by the increment.
		if sequenceVal > oldStart {
			err := setSequenceVal(oldStart - opts.Increment)
			if err != nil {
				return err
			}
		}

		// If the sequence were exhausted, it would be beyond its previous bounds.
		if sequenceVal < oldMinValue {
			// Every call to nextval increments the sequence even if the
			// sequence is exhausted. Find the final valid value of the sequence
			// by calculating the number of valid calls to it
			calls := (oldMinValue - oldStart) / oldIncrement
			err := setSequenceVal(oldStart + calls*oldIncrement)
			if err != nil {
				return err
			}
		}
	} else if opts.Increment > 0 && (oldIncrement != opts.Increment || opts.MaxValue > oldMaxValue) {
		sequenceVal, err := getSequenceValue()
		if err != nil {
			return err
		}

		if sequenceVal < oldStart {
			err := setSequenceVal(oldStart - opts.Increment)
			if err != nil {
				return err
			}
		}

		if sequenceVal > oldMaxValue {
			calls := (oldMaxValue - oldStart) / oldIncrement
			err := setSequenceVal(oldStart + calls*oldIncrement)
			if err != nil {
				return err
			}
		}
	}

	var restartVal *int64
	for _, option := range seqOptions {
		if option.Name == tree.SeqOptRestart {
			// If the RESTART option is present but without a value, then use the
			// START WITH value.
			if option.IntVal != nil {
				restartVal = option.IntVal
			} else {
				restartVal = &opts.Start
			}
		}
	}
	if restartVal != nil {
		err := setSequenceVal(*restartVal - opts.Increment)
		if err != nil {
			return err
		}
	}

	if err := params.p.writeSchemaChange(
		params.ctx, seqDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(formatter, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (n *alterSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterSequenceNode) Close(context.Context)        {}
