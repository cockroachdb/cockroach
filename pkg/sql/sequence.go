// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

// GetSerialSequenceNameFromColumn is part of the tree.SequenceOperators interface.
func (p *planner) GetSerialSequenceNameFromColumn(
	ctx context.Context, tn *tree.TableName, columnName tree.Name,
) (*tree.TableName, error) {
	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireTableDesc)
	tableDesc, err := resolver.ResolveExistingTableObject(ctx, p, tn, flags)
	if err != nil {
		return nil, err
	}
	for _, col := range tableDesc.Columns {
		if col.ColName() == columnName {
			// Seems like we have no way of detecting whether this was done using "SERIAL".
			// Guess by assuming it is SERIAL it it uses only one sequence.
			// NOTE: This could be alleviated by going through the process of saving SERIAL
			//       into the descriptor for the column, but has flow on effects for
			//       which have not been thought about (e.g. implication for backup and restore,
			//       as well as backward compatibility) so we're using this heuristic for now.
			// TODO(#52487): fix this up.
			if len(col.UsesSequenceIds) == 1 {
				seq, err := p.Descriptors().GetImmutableTableByID(
					ctx,
					p.txn,
					col.UsesSequenceIds[0],
					tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc),
				)
				if err != nil {
					return nil, err
				}
				return p.getQualifiedTableName(ctx, seq)
			}
			return nil, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(columnName))
}

// IncrementSequence implements the tree.SequenceOperators interface.
func (p *planner) IncrementSequence(ctx context.Context, seqName *tree.TableName) (int64, error) {
	if p.EvalContext().TxnReadOnly {
		return 0, readOnlyError("nextval()")
	}

	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc)
	descriptor, err := resolver.ResolveExistingTableObject(ctx, p, seqName, flags)
	if err != nil {
		return 0, err
	}
	if err := p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
		return 0, err
	}

	seqOpts := descriptor.SequenceOpts
	var val int64
	if seqOpts.Virtual {
		rowid := builtins.GenerateUniqueInt(p.EvalContext().NodeID.SQLInstanceID())
		val = int64(rowid)
	} else {
		seqValueKey := p.ExecCfg().Codec.SequenceKey(uint32(descriptor.ID))
		val, err = kv.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, seqOpts.Increment)
		if err != nil {
			if errors.HasType(err, (*roachpb.IntegerOverflowError)(nil)) {
				return 0, boundsExceededError(descriptor)
			}
			return 0, err
		}
		if val > seqOpts.MaxValue || val < seqOpts.MinValue {
			return 0, boundsExceededError(descriptor)
		}
	}

	p.ExtendedEvalContext().SessionMutator.RecordLatestSequenceVal(uint32(descriptor.ID), val)

	return val, nil
}

func boundsExceededError(descriptor *tabledesc.Immutable) error {
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
	return pgerror.Newf(
		pgcode.SequenceGeneratorLimitExceeded,
		`reached %s value of sequence %q (%d)`, word,
		tree.ErrString((*tree.Name)(&descriptor.Name)), value)
}

// GetLatestValueInSessionForSequence implements the tree.SequenceOperators interface.
func (p *planner) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc)
	descriptor, err := resolver.ResolveExistingTableObject(ctx, p, seqName, flags)
	if err != nil {
		return 0, err
	}

	val, ok := p.SessionData().SequenceState.GetLastValueByID(uint32(descriptor.ID))
	if !ok {
		return 0, pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			`currval of sequence %q is not yet defined in this session`, tree.ErrString(seqName))
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

	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc)
	descriptor, err := resolver.ResolveExistingTableObject(ctx, p, seqName, flags)
	if err != nil {
		return err
	}
	if err := p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
		return err
	}

	if descriptor.SequenceOpts.Virtual {
		// TODO(knz): we currently return an error here, but if/when
		// CockroachDB grows to automatically make sequences virtual when
		// clients don't expect it, we may need to make this a no-op
		// instead.
		return pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			`cannot set the value of virtual sequence %q`, tree.ErrString(seqName))
	}

	seqValueKey, newVal, err := MakeSequenceKeyVal(p.ExecCfg().Codec, descriptor, newVal, isCalled)
	if err != nil {
		return err
	}

	// TODO(vilterp): not supposed to mix usage of Inc and Put on a key,
	// according to comments on Inc operation. Switch to Inc if `desired-current`
	// overflows correctly.
	return p.txn.Put(ctx, seqValueKey, newVal)
}

// MakeSequenceKeyVal returns the key and value of a sequence being set
// with newVal.
func MakeSequenceKeyVal(
	codec keys.SQLCodec, sequence catalog.TableDescriptor, newVal int64, isCalled bool,
) ([]byte, int64, error) {
	opts := sequence.GetSequenceOpts()
	if newVal > opts.MaxValue || newVal < opts.MinValue {
		return nil, 0, pgerror.Newf(
			pgcode.NumericValueOutOfRange,
			`value %d is out of bounds for sequence "%s" (%d..%d)`,
			newVal, sequence.GetName(), opts.MinValue, opts.MaxValue,
		)
	}
	if !isCalled {
		newVal = newVal - opts.Increment
	}

	seqValueKey := codec.SequenceKey(uint32(sequence.GetID()))
	return seqValueKey, newVal, nil
}

// GetSequenceValue returns the current value of the sequence.
func (p *planner) GetSequenceValue(
	ctx context.Context, codec keys.SQLCodec, desc *tabledesc.Immutable,
) (int64, error) {
	if desc.SequenceOpts == nil {
		return 0, errors.New("descriptor is not a sequence")
	}
	keyValue, err := p.txn.Get(ctx, codec.SequenceKey(uint32(desc.ID)))
	if err != nil {
		return 0, err
	}
	return keyValue.ValueInt(), nil
}

func readOnlyError(s string) error {
	return pgerror.Newf(pgcode.ReadOnlySQLTransaction,
		"cannot execute %s in a read-only transaction", s)
}

// assignSequenceOptions moves options from the AST node to the sequence options descriptor,
// starting with defaults and overriding them with user-provided options.
func assignSequenceOptions(
	opts *descpb.TableDescriptor_SequenceOpts,
	optsNode tree.SequenceOptions,
	setDefaults bool,
	params *runParams,
	sequenceID descpb.ID,
	sequenceParentID descpb.ID,
) error {
	// All other defaults are dependent on the value of increment,
	// i.e. whether the sequence is ascending or descending.
	for _, option := range optsNode {
		if option.Name == tree.SeqOptIncrement {
			opts.Increment = *option.IntVal
		}
	}
	if opts.Increment == 0 {
		return pgerror.New(
			pgcode.InvalidParameterValue, "INCREMENT must not be zero")
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
			return pgerror.New(pgcode.Syntax, "conflicting or redundant options")
		}
		optionsSeen[option.Name] = true

		switch option.Name {
		case tree.SeqOptCycle:
			return unimplemented.NewWithIssue(20961,
				"CYCLE option is not supported")
		case tree.SeqOptNoCycle:
			// Do nothing; this is the default.
		case tree.SeqOptCache:
			v := *option.IntVal
			switch {
			case v < 1:
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"CACHE (%d) must be greater than zero", v)
			case v == 1:
				// Do nothing; this is the default.
			case v > 1:
				return unimplemented.NewWithIssuef(32567,
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
		case tree.SeqOptVirtual:
			opts.Virtual = true
		case tree.SeqOptOwnedBy:
			if params == nil {
				return pgerror.Newf(pgcode.Internal,
					"Trying to add/remove Sequence Owner without access to context")
			}
			// The owner is being removed
			if option.ColumnItemVal == nil {
				if err := removeSequenceOwnerIfExists(params.ctx, params.p, sequenceID, opts); err != nil {
					return err
				}
			} else {
				// The owner is being added/modified
				tableDesc, col, err := resolveColumnItemToDescriptors(
					params.ctx, params.p, option.ColumnItemVal,
				)
				if err != nil {
					return err
				}
				if tableDesc.ParentID != sequenceParentID &&
					!allowCrossDatabaseSeqOwner.Get(&params.p.execCfg.Settings.SV) {
					return pgerror.Newf(pgcode.FeatureNotSupported,
						"OWNED BY cannot refer to other databases; (see the '%s' cluster setting)",
						allowCrossDatabaseSeqOwnerSetting,
					)
				}
				// We only want to trigger schema changes if the owner is not what we
				// want it to be.
				if opts.SequenceOwner.OwnerTableID != tableDesc.ID ||
					opts.SequenceOwner.OwnerColumnID != col.ID {
					if err := removeSequenceOwnerIfExists(params.ctx, params.p, sequenceID, opts); err != nil {
						return err
					}
					err := addSequenceOwner(params.ctx, params.p, option.ColumnItemVal, sequenceID, opts)
					if err != nil {
						return err
					}
				}
			}
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
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"START value (%d) cannot be greater than MAXVALUE (%d)", opts.Start, opts.MaxValue)
	}
	if opts.Start < opts.MinValue {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"START value (%d) cannot be less than MINVALUE (%d)", opts.Start, opts.MinValue)
	}

	return nil
}

func removeSequenceOwnerIfExists(
	ctx context.Context, p *planner, sequenceID descpb.ID, opts *descpb.TableDescriptor_SequenceOpts,
) error {
	if !opts.HasOwner() {
		return nil
	}
	tableDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, opts.SequenceOwner.OwnerTableID, p.txn)
	if err != nil {
		// Special case error swallowing for #50711 and #50781, which can cause a
		// column to own sequences that have been dropped/do not exist.
		if errors.Is(err, catalog.ErrDescriptorDropped) ||
			pgerror.GetPGCode(err) == pgcode.UndefinedTable {
			log.Eventf(ctx, "swallowing error during sequence ownership unlinking: %s", err.Error())
			return nil
		}
		return err
	}
	// If the table descriptor has already been dropped, there is no need to
	// remove the reference.
	if tableDesc.Dropped() {
		return nil
	}
	col, err := tableDesc.FindColumnByID(opts.SequenceOwner.OwnerColumnID)
	if err != nil {
		return err
	}
	// Find an item in colDesc.OwnsSequenceIds which references SequenceID.
	refIdx := -1
	for i, id := range col.OwnsSequenceIds {
		if id == sequenceID {
			refIdx = i
		}
	}
	if refIdx == -1 {
		return errors.AssertionFailedf("couldn't find reference from column to this sequence")
	}
	col.OwnsSequenceIds = append(col.OwnsSequenceIds[:refIdx], col.OwnsSequenceIds[refIdx+1:]...)
	if err := p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID,
		fmt.Sprintf("removing sequence owner %s(%d) for sequence %d",
			tableDesc.Name, tableDesc.ID, sequenceID,
		),
	); err != nil {
		return err
	}
	// Reset the SequenceOwner to empty
	opts.SequenceOwner.Reset()
	return nil
}

func resolveColumnItemToDescriptors(
	ctx context.Context, p *planner, columnItem *tree.ColumnItem,
) (*tabledesc.Mutable, *descpb.ColumnDescriptor, error) {
	var tableName tree.TableName
	if columnItem.TableName != nil {
		tableName = columnItem.TableName.ToTableName()
	}

	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &tableName, true /* required */, tree.ResolveRequireTableDesc)
	if err != nil {
		return nil, nil, err
	}
	col, _, err := tableDesc.FindColumnByName(columnItem.ColumnName)
	if err != nil {
		return nil, nil, err
	}
	return tableDesc, col, nil
}

func addSequenceOwner(
	ctx context.Context,
	p *planner,
	columnItemVal *tree.ColumnItem,
	sequenceID descpb.ID,
	opts *descpb.TableDescriptor_SequenceOpts,
) error {
	tableDesc, col, err := resolveColumnItemToDescriptors(ctx, p, columnItemVal)
	if err != nil {
		return err
	}

	col.OwnsSequenceIds = append(col.OwnsSequenceIds, sequenceID)

	opts.SequenceOwner.OwnerColumnID = col.ID
	opts.SequenceOwner.OwnerTableID = tableDesc.GetID()
	return p.writeSchemaChange(
		ctx, tableDesc, descpb.InvalidMutationID, fmt.Sprintf(
			"adding sequence owner %s(%d) for sequence %d",
			tableDesc.Name, tableDesc.ID, sequenceID),
	)
}

// maybeAddSequenceDependencies adds references between the column and sequence descriptors,
// if the column has a DEFAULT expression that uses one or more sequences. (Usually just one,
// e.g. `DEFAULT nextval('my_sequence')`.
// The passed-in column descriptor is mutated, and the modified sequence descriptors are returned.
func maybeAddSequenceDependencies(
	ctx context.Context,
	sc resolver.SchemaResolver,
	tableDesc *tabledesc.Mutable,
	col *descpb.ColumnDescriptor,
	expr tree.TypedExpr,
	backrefs map[descpb.ID]*tabledesc.Mutable,
) ([]*tabledesc.Mutable, error) {
	seqNames, err := sequence.GetUsedSequenceNames(expr)
	if err != nil {
		return nil, err
	}
	var seqDescs []*tabledesc.Mutable
	for _, seqName := range seqNames {
		parsedSeqName, err := parser.ParseTableName(seqName)
		if err != nil {
			return nil, err
		}
		tn := parsedSeqName.ToTableName()

		var seqDesc *tabledesc.Mutable
		p, ok := sc.(*planner)
		if ok {
			seqDesc, err = p.ResolveMutableTableDescriptor(ctx, &tn, true /*required*/, tree.ResolveRequireSequenceDesc)
			if err != nil {
				return nil, err
			}
		} else {
			// This is only executed via IMPORT which uses its own resolver.
			seqDesc, err = resolver.ResolveMutableExistingTableObject(ctx, sc, &tn, true /*required*/, tree.ResolveRequireSequenceDesc)
			if err != nil {
				return nil, err
			}
		}
		// If we had already modified this Sequence as part of this transaction,
		// we only want to modify a single instance of it instead of overwriting it.
		// So replace seqDesc with the descriptor that was previously modified.
		if prev, ok := backrefs[seqDesc.ID]; ok {
			seqDesc = prev
		}
		col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.ID)
		// Add reference from sequence descriptor to column.
		refIdx := -1
		for i, reference := range seqDesc.DependedOnBy {
			if reference.ID == tableDesc.ID {
				refIdx = i
			}
		}
		if refIdx == -1 {
			seqDesc.DependedOnBy = append(seqDesc.DependedOnBy, descpb.TableDescriptor_Reference{
				ID:        tableDesc.ID,
				ColumnIDs: []descpb.ColumnID{col.ID},
			})
		} else {
			seqDesc.DependedOnBy[refIdx].ColumnIDs = append(seqDesc.DependedOnBy[refIdx].ColumnIDs, col.ID)
		}
		seqDescs = append(seqDescs, seqDesc)
	}
	return seqDescs, nil
}

// dropSequencesOwnedByCol drops all the sequences from col.OwnsSequenceIDs.
// Called when the respective column (or the whole table) is being dropped.
func (p *planner) dropSequencesOwnedByCol(
	ctx context.Context, col *descpb.ColumnDescriptor, queueJob bool,
) error {
	// Copy out the sequence IDs as the code to drop the sequence will reach
	// back around and update the descriptor from underneath us.
	ownsSequenceIDs := append([]descpb.ID(nil), col.OwnsSequenceIds...)
	for _, sequenceID := range ownsSequenceIDs {
		seqDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, sequenceID, p.txn)
		// Special case error swallowing for #50781, which can cause a
		// column to own sequences that do not exist.
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorDropped) ||
				pgerror.GetPGCode(err) == pgcode.UndefinedTable {
				log.Eventf(ctx, "swallowing error dropping owned sequences: %s", err.Error())
				continue
			}
			return err
		}
		// This sequence is already getting dropped. Don't do it twice.
		if seqDesc.Dropped() {
			continue
		}
		jobDesc := fmt.Sprintf("removing sequence %q dependent on column %q which is being dropped",
			seqDesc.Name, col.ColName())
		// Note that this call will end up resolving and modifying the table
		// descriptor.
		if err := p.dropSequenceImpl(
			ctx, seqDesc, queueJob, jobDesc, tree.DropRestrict,
		); err != nil {
			return err
		}
	}
	return nil
}

// removeSequenceDependencies:
//   - removes the reference from the column descriptor to the sequence descriptor.
//   - removes the reference from the sequence descriptor to the column descriptor.
//   - writes the sequence descriptor and notifies a schema change.
// The column descriptor is mutated but not saved to persistent storage; the caller must save it.
func (p *planner) removeSequenceDependencies(
	ctx context.Context, tableDesc *tabledesc.Mutable, col *descpb.ColumnDescriptor,
) error {
	for _, sequenceID := range col.UsesSequenceIds {
		// Get the sequence descriptor so we can remove the reference from it.
		seqDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, sequenceID, p.txn)
		if err != nil {
			return err
		}
		// If the sequence descriptor has been dropped, we do not need to unlink the
		// dependency. This can happen during a `DROP DATABASE CASCADE` when both
		// the table and sequence are objects in the database being dropped. If
		// `dropImpl` is called on the sequence before the table, because CRDB
		// doesn't implement CASCADE for sequences, the dependency to the
		// table descriptor is not unlinked. This check prevents us from failing
		// when trying to unlink a dependency that really shouldn't have existed
		// at this point in the code to begin with.
		if seqDesc.Dropped() {
			continue
		}
		// Find the item in seqDesc.DependedOnBy which references tableDesc and col.
		refTableIdx := -1
		refColIdx := -1
	found:
		for i, reference := range seqDesc.DependedOnBy {
			if reference.ID == tableDesc.ID {
				refTableIdx = i
				for j, colRefID := range seqDesc.DependedOnBy[i].ColumnIDs {
					if colRefID == col.ID {
						refColIdx = j
						break found
					}
					// Before #40852, columnIDs stored in the SeqDesc were 0 as they hadn't
					// been allocated then. The 0 check prevents older descs from breaking.
					// Do not break though, as we still want to search in case the actual ID
					// exists.
					if colRefID == 0 {
						refColIdx = j
					}
				}
			}
		}
		if refColIdx == -1 {
			return errors.AssertionFailedf("couldn't find reference from sequence to this column")
		}
		// Remove the column ID from the sequence descriptors list of things that
		// depend on it. If the column was the only column that depended on the
		// sequence, remove the table reference from the sequence as well.
		seqDesc.DependedOnBy[refTableIdx].ColumnIDs = append(
			seqDesc.DependedOnBy[refTableIdx].ColumnIDs[:refColIdx],
			seqDesc.DependedOnBy[refTableIdx].ColumnIDs[refColIdx+1:]...)

		if len(seqDesc.DependedOnBy[refTableIdx].ColumnIDs) == 0 {
			seqDesc.DependedOnBy = append(
				seqDesc.DependedOnBy[:refTableIdx],
				seqDesc.DependedOnBy[refTableIdx+1:]...)
		}

		jobDesc := fmt.Sprintf("removing sequence %q dependent on column %q which is being dropped",
			seqDesc.Name, col.ColName())
		if err := p.writeSchemaChange(
			ctx, seqDesc, descpb.InvalidMutationID, jobDesc,
		); err != nil {
			return err
		}
	}
	// Remove the reference from the column descriptor to the sequence descriptor.
	col.UsesSequenceIds = []descpb.ID{}
	return nil
}
