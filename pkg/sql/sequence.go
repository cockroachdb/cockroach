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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

	p.session.dataMutator.RecordLatestSequenceVal(uint32(descriptor.ID), val)

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
	// TODO(vilterp): not supposed to mix usage of Inc and Put on a key,
	// according to comments on Inc operation. Switch to Inc if `desired-current`
	// overflows correctly.
	return p.txn.Put(ctx, seqValueKey, newVal)
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

// maybeAddSequenceDependency:
// - if the given expr uses a sequence
//   - adds a reference on `col` to the sequence. (mutates `col`; caller must persist changes to KV)
//   - adds a reference on the sequence descriptor to `col`. (returns the modified sequence
//     descriptor for the caller to save)
func maybeAddSequenceDependency(
	tableDesc *sqlbase.TableDescriptor,
	col *sqlbase.ColumnDescriptor,
	expr tree.TypedExpr,
	evalCtx *tree.EvalContext,
) (*sqlbase.TableDescriptor, error) {
	ctx := evalCtx.Ctx()
	seqName, err := getUsedSequenceName(expr)
	if err != nil {
		return nil, err
	}
	if seqName == nil {
		return nil, nil
	}
	parsedSeqName, err := evalCtx.Planner.ParseQualifiedTableName(ctx, *seqName)
	if err != nil {
		return nil, err
	}
	seqDesc, err := getSequenceDesc(ctx, evalCtx.Txn, NilVirtualTabler, parsedSeqName)
	if err != nil {
		return nil, err
	}
	col.UsesSequenceId = &seqDesc.ID
	// Now that we've gotten the sequence descriptor, I guess we can mutate it?
	seqDesc.DependedOnBy = append(seqDesc.DependedOnBy, sqlbase.TableDescriptor_Reference{
		ID:        tableDesc.ID,
		ColumnIDs: []sqlbase.ColumnID{col.ID},
	})
	return seqDesc, nil
}

// removeSequenceDependency:
//   - removes the reference from the column descriptor to the sequence descriptor.
//   - removes the reference from the sequence descriptor to the column descriptor.
//   - writes the sequence descriptor and notifies a schema change.
// The column is mutated but not saved to persistent storage; the caller must save
// the modified column descriptor.
func removeSequenceDependency(
	tableDesc *sqlbase.TableDescriptor, col *sqlbase.ColumnDescriptor, params runParams,
) error {
	// Get the sequence descriptor so we can remove the reference from it.
	seqDesc := sqlbase.TableDescriptor{}
	if err := getDescriptorByID(params.ctx, params.p.txn, *col.UsesSequenceId, &seqDesc); err != nil {
		return err
	}
	// Find an item in seqDesc.DependedOnBy which references tableDesc.
	refIdx := -1
	for i := 0; i < len(seqDesc.DependedOnBy); i++ {
		reference := seqDesc.DependedOnBy[i]
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
	// Remove the reference from the column descriptor to the sequence descriptor.
	col.UsesSequenceId = nil
	return nil
}

// getUsedSequenceName returns the name of the sequence passed to
// a call to nextval in the given expression, or nil if there is
// no call to nextval.
// e.g. nextval('foo') => "foo"; <some other expression> => nil
func getUsedSequenceName(defaultExpr tree.TypedExpr) (*string, error) {
	searchPath := sessiondata.SearchPath{}
	var sequenceName *string
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
						asString := string(*a)
						sequenceName = &asString
					}
				}
			}
			return nil, true, expr
		},
	)
	if err != nil {
		return nil, err
	}
	return sequenceName, nil
}
