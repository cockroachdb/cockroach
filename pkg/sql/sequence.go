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
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// IncrementSequence implements the tree.EvalPlanner interface.
func (p *planner) IncrementSequence(ctx context.Context, seqName *tree.TableName) (int64, error) {
	if p.EvalContext().TxnReadOnly {
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
		switch err.(type) {
		case *roachpb.IntegerOverflowError:
			return 0, boundsExceededError(descriptor)
		default:
			return 0, err
		}
	}

	seqOpts := descriptor.SequenceOpts
	if val > seqOpts.MaxValue || val < seqOpts.MinValue {
		return 0, boundsExceededError(descriptor)
	}

	p.ExtendedEvalContext().SessionMutator.RecordLatestSequenceVal(uint32(descriptor.ID), val)

	return val, nil
}

func boundsExceededError(descriptor *sqlbase.TableDescriptor) error {
	seqOpts := descriptor.SequenceOpts
	isAscending := seqOpts.Increment > 0

	var word string
	var value int64
	if isAscending {
		word = "maximum"
		value = seqOpts.MaxValue
	} else {
		word = "minimum"
		value = seqOpts.MinValue
	}
	return pgerror.NewErrorf(
		pgerror.CodeSequenceGeneratorLimitExceeded,
		`reached %s value of sequence "%s" (%d)`, word, descriptor.Name, value)
}

// GetLatestValueInSessionForSequence implements the tree.EvalPlanner interface.
func (p *planner) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	descriptor, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), seqName)
	if err != nil {
		return 0, err
	}

	val, ok := p.SessionData().SequenceState.GetLastValueByID(uint32(descriptor.ID))
	if !ok {
		return 0, pgerror.NewErrorf(
			pgerror.CodeObjectNotInPrerequisiteStateError,
			`currval of sequence "%s" is not yet defined in this session`, seqName)
	}

	return val, nil
}

// SetSequenceValue implements the tree.EvalPlanner interface.
func (p *planner) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	if p.EvalContext().TxnReadOnly {
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
	if !isCalled {
		newVal = newVal - descriptor.SequenceOpts.Increment
	}
	seqValueKey := keys.MakeSequenceKey(uint32(descriptor.ID))
	// TODO(vilterp): not supposed to mix usage of Inc and Put on a key,
	// according to comments on Inc operation. Switch to Inc if `desired-current`
	// overflows correctly.
	return p.txn.Put(ctx, seqValueKey, newVal)
}

// GetSequenceValue returns the current value of the sequence.
func (p *planner) GetSequenceValue(
	ctx context.Context, desc *sqlbase.TableDescriptor,
) (int64, error) {
	if desc.SequenceOpts == nil {
		return 0, errors.New("descriptor is not a sequence")
	}
	keyValue, err := p.txn.Get(ctx, keys.MakeSequenceKey(uint32(desc.ID)))
	if err != nil {
		return 0, err
	}
	return keyValue.ValueInt(), nil
}

func readOnlyError(s string) error {
	return pgerror.NewErrorf(pgerror.CodeReadOnlySQLTransactionError,
		"cannot execute %s in a read-only transaction", s)
}

// assignSequenceOptions moves options from the AST node to the sequence options descriptor,
// starting with defaults and overriding them with user-provided options.
func assignSequenceOptions(
	opts *sqlbase.TableDescriptor_SequenceOpts, optsNode tree.SequenceOptions, setDefaults bool,
) error {
	// All other defaults are dependent on the value of increment,
	// i.e. whether the sequence is ascending or descending.
	for _, option := range optsNode {
		if option.Name == tree.SeqOptIncrement {
			opts.Increment = *option.IntVal
		}
	}
	if opts.Increment == 0 {
		return pgerror.NewError(
			pgerror.CodeInvalidParameterValueError, "INCREMENT must not be zero")
	}
	isAscending := opts.Increment > 0

	// Set increment-dependent defaults.
	if setDefaults {
		if isAscending {
			opts.MinValue = 1
			opts.MaxValue = math.MaxInt64
			opts.Start = opts.MinValue
		} else {
			opts.MinValue = math.MinInt64
			opts.MaxValue = -1
			opts.Start = opts.MaxValue
		}
	}

	// Fill in all other options.
	optionsSeen := map[string]bool{}
	for _, option := range optsNode {
		// Error on duplicate options.
		_, seenBefore := optionsSeen[option.Name]
		if seenBefore {
			return pgerror.NewError(pgerror.CodeSyntaxError, "conflicting or redundant options")
		}
		optionsSeen[option.Name] = true

		switch option.Name {
		case tree.SeqOptIncrement:
			// Do nothing; this has already been set.
		case tree.SeqOptMinValue:
			// A value of nil represents the user explicitly saying `NO MINVALUE`.
			if option.IntVal != nil {
				opts.MinValue = *option.IntVal
			}
		case tree.SeqOptMaxValue:
			// A value of nil represents the user explicitly saying `NO MAXVALUE`.
			if option.IntVal != nil {
				opts.MaxValue = *option.IntVal
			}
		case tree.SeqOptStart:
			opts.Start = *option.IntVal
		}
	}

	// If start option not specified, set it to MinValue (for ascending sequences)
	// or MaxValue (for descending sequences).
	if _, startSeen := optionsSeen[tree.SeqOptStart]; !startSeen {
		if opts.Increment > 0 {
			opts.Start = opts.MinValue
		} else {
			opts.Start = opts.MaxValue
		}
	}

	if opts.Start > opts.MaxValue {
		return pgerror.NewErrorf(
			pgerror.CodeInvalidParameterValueError,
			"START value (%d) cannot be greater than MAXVALUE (%d)", opts.Start, opts.MaxValue)
	}
	if opts.Start < opts.MinValue {
		return pgerror.NewErrorf(
			pgerror.CodeInvalidParameterValueError,
			"START value (%d) cannot be less than MINVALUE (%d)", opts.Start, opts.MinValue)
	}

	return nil
}
