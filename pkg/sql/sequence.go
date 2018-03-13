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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// IncrementSequence implements the tree.SequenceOperators interface.
func (p *planner) IncrementSequence(ctx context.Context, seqName *tree.TableName) (int64, error) {
	if p.EvalContext().TxnReadOnly {
		return 0, readOnlyError("nextval()")
	}

	// TODO(vivek,knz): this lookup should really use the cached descriptor.
	// However tests break if it does.
	var descriptor *TableDescriptor
	var err error
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptor, err = ResolveExistingObject(ctx, p, seqName, true /*required*/, requireSequenceDesc)
	})
	if err != nil {
		return 0, err
	}
	if err := p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
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

// GetLatestValueInSessionForSequence implements the tree.SequenceOperators interface.
func (p *planner) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	// TODO(vivek,knz): this lookup should really use the cached descriptor.
	// However tests break if it does.
	var descriptor *TableDescriptor
	var err error
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		descriptor, err = ResolveExistingObject(ctx, p, seqName, true /*required*/, requireSequenceDesc)
	})
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

// SetSequenceValue implements the tree.SequenceOperators interface.
func (p *planner) SetSequenceValue(
	ctx context.Context, seqName *tree.TableName, newVal int64, isCalled bool,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("setval()")
	}

	// TODO(vivek,knz): this lookup should really use the cached descriptor.
	// However tests break if it does.
	var descriptor *TableDescriptor
	var err error
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		descriptor, err = ResolveExistingObject(ctx, p, seqName, true /*required*/, requireSequenceDesc)
	})
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
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
		case tree.SeqOptCycle:
			return pgerror.NewError(pgerror.CodeFeatureNotSupportedError,
				"CYCLE option is not supported")
		case tree.SeqOptNoCycle:
			// Do nothing; this is the default.
		case tree.SeqOptCache:
			v := *option.IntVal
			switch {
			case v < 1:
				return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
					"CACHE (%d) must be greater than zero", v)
			case v == 1:
				// Do nothing; this is the default.
			case v > 1:
				return pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
					"CACHE values larger than 1 are not supported, found %d", v)
			}
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

// maybeAddSequenceDependencies adds references between the column and sequence descriptors,
// if the column has a DEFAULT expression that uses one or more sequences. (Usually just one,
// e.g. `DEFAULT nextval('my_sequence')`.
// The passed-in column descriptor is mutated, and the modified sequence descriptors are returned.
func maybeAddSequenceDependencies(
	sc SchemaResolver,
	tableDesc *sqlbase.TableDescriptor,
	col *sqlbase.ColumnDescriptor,
	expr tree.TypedExpr,
	evalCtx *tree.EvalContext,
) ([]*sqlbase.TableDescriptor, error) {
	ctx := evalCtx.Ctx()
	seqNames, err := getUsedSequenceNames(expr)
	if err != nil {
		return nil, err
	}
	var seqDescs []*sqlbase.TableDescriptor
	for _, seqName := range seqNames {
		parsedSeqName, err := evalCtx.Sequence.ParseQualifiedTableName(ctx, seqName)
		if err != nil {
			return nil, err
		}
		seqDesc, err := ResolveExistingObject(ctx, sc, parsedSeqName, true /*required*/, requireSequenceDesc)
		if err != nil {
			return nil, err
		}
		col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.ID)
		// Add reference from sequence descriptor to column.
		seqDesc.DependedOnBy = append(seqDesc.DependedOnBy, sqlbase.TableDescriptor_Reference{
			ID:        tableDesc.ID,
			ColumnIDs: []sqlbase.ColumnID{col.ID},
		})
		seqDescs = append(seqDescs, seqDesc)
	}
	return seqDescs, nil
}

// removeSequenceDependencies:
//   - removes the reference from the column descriptor to the sequence descriptor.
//   - removes the reference from the sequence descriptor to the column descriptor.
//   - writes the sequence descriptor and notifies a schema change.
// The column descriptor is mutated but not saved to persistent storage; the caller must save it.
func removeSequenceDependencies(
	tableDesc *sqlbase.TableDescriptor, col *sqlbase.ColumnDescriptor, params runParams,
) error {
	for _, sequenceID := range col.UsesSequenceIds {
		// Get the sequence descriptor so we can remove the reference from it.
		seqDesc := sqlbase.TableDescriptor{}
		if err := getDescriptorByID(params.ctx, params.p.txn, sequenceID, &seqDesc); err != nil {
			return err
		}
		// Find an item in seqDesc.DependedOnBy which references tableDesc.
		refIdx := -1
		for i, reference := range seqDesc.DependedOnBy {
			if reference.ID == tableDesc.ID {
				refIdx = i
			}
		}
		if refIdx == -1 {
			return pgerror.NewError(
				pgerror.CodeInternalError, "couldn't find reference from sequence to this column")
		}
		seqDesc.DependedOnBy = append(seqDesc.DependedOnBy[:refIdx], seqDesc.DependedOnBy[refIdx+1:]...)
		if err := params.p.writeTableDesc(params.ctx, &seqDesc); err != nil {
			return err
		}
		params.p.notifySchemaChange(&seqDesc, sqlbase.InvalidMutationID)
	}
	// Remove the reference from the column descriptor to the sequence descriptor.
	col.UsesSequenceIds = []sqlbase.ID{}
	return nil
}

// getUsedSequenceNames returns the name of the sequence passed to
// a call to nextval in the given expression, or nil if there is
// no call to nextval.
// e.g. nextval('foo') => "foo"; <some other expression> => nil
func getUsedSequenceNames(defaultExpr tree.TypedExpr) ([]string, error) {
	searchPath := sessiondata.SearchPath{}
	var names []string
	_, err := tree.SimpleVisit(
		defaultExpr,
		func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
			switch t := expr.(type) {
			case *tree.FuncExpr:
				def, err := t.Func.Resolve(searchPath)
				if err != nil {
					return err, false, expr
				}
				if def.Name == "nextval" {
					arg := t.Exprs[0]
					switch a := arg.(type) {
					case *tree.DString:
						names = append(names, string(*a))
					}
				}
			}
			return nil, true, expr
		},
	)
	if err != nil {
		return nil, err
	}
	return names, nil
}
