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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
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
	_, tableDesc, err := resolver.ResolveExistingTableObject(ctx, p, tn, flags)
	if err != nil {
		return nil, err
	}
	for _, col := range tableDesc.PublicColumns() {
		if col.ColName() == columnName {
			// Seems like we have no way of detecting whether this was done using "SERIAL".
			// Guess by assuming it is SERIAL it it uses only one sequence.
			// NOTE: This could be alleviated by going through the process of saving SERIAL
			//       into the descriptor for the column, but has flow on effects for
			//       which have not been thought about (e.g. implication for backup and restore,
			//       as well as backward compatibility) so we're using this heuristic for now.
			// TODO(#52487): fix this up.
			if col.NumUsesSequences() == 1 {
				seq, err := p.Descriptors().GetImmutableTableByID(
					ctx,
					p.txn,
					col.GetUsesSequenceID(0),
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
	_, descriptor, err := resolver.ResolveExistingTableObject(ctx, p, seqName, flags)
	if err != nil {
		return 0, err
	}
	return incrementSequenceHelper(ctx, p, descriptor)
}

// IncrementSequenceByID implements the tree.SequenceOperators interface.
func (p *planner) IncrementSequenceByID(ctx context.Context, seqID int64) (int64, error) {
	if p.EvalContext().TxnReadOnly {
		return 0, readOnlyError("nextval()")
	}
	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc)
	descriptor, err := p.Descriptors().GetImmutableTableByID(ctx, p.txn, descpb.ID(seqID), flags)
	if err != nil {
		return 0, err
	}
	if !descriptor.IsSequence() {
		seqName, err := p.getQualifiedTableName(ctx, descriptor)
		if err != nil {
			return 0, err
		}
		return 0, sqlerrors.NewWrongObjectTypeError(seqName, "sequence")
	}
	return incrementSequenceHelper(ctx, p, descriptor)
}

// incrementSequenceHelper is shared by IncrementSequence and IncrementSequenceByID
// to increment the given sequence.
func incrementSequenceHelper(
	ctx context.Context, p *planner, descriptor catalog.TableDescriptor,
) (int64, error) {
	if err := p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
		return 0, err
	}

	seqOpts := descriptor.GetSequenceOpts()

	var val int64
	var err error
	if seqOpts.Virtual {
		rowid := builtins.GenerateUniqueInt(p.EvalContext().NodeID.SQLInstanceID())
		val = int64(rowid)
	} else {
		val, err = p.incrementSequenceUsingCache(ctx, descriptor)
	}
	if err != nil {
		return 0, err
	}

	p.ExtendedEvalContext().SessionMutator.RecordLatestSequenceVal(uint32(descriptor.GetID()), val)

	return val, nil
}

// incrementSequenceUsingCache fetches the next value of the sequence
// represented by the passed catalog.TableDescriptor. If the sequence has a
// cache size of greater than 1, then this function will read cached values
// from the session data and repopulate these values when the cache is empty.
func (p *planner) incrementSequenceUsingCache(
	ctx context.Context, descriptor catalog.TableDescriptor,
) (int64, error) {
	seqOpts := descriptor.GetSequenceOpts()

	cacheSize := seqOpts.EffectiveCacheSize()

	fetchNextValues := func() (currentValue, incrementAmount, sizeOfCache int64, err error) {
		seqValueKey := p.ExecCfg().Codec.SequenceKey(uint32(descriptor.GetID()))

		endValue, err := kv.IncrementValRetryable(
			ctx, p.txn.DB(), seqValueKey, seqOpts.Increment*cacheSize)

		if err != nil {
			if errors.HasType(err, (*roachpb.IntegerOverflowError)(nil)) {
				return 0, 0, 0, boundsExceededError(descriptor)
			}
			return 0, 0, 0, err
		}

		// This sequence has exceeded its bounds after performing this increment.
		if endValue > seqOpts.MaxValue || endValue < seqOpts.MinValue {
			// If the sequence exceeded its bounds prior to the increment, then return an error.
			if (seqOpts.Increment > 0 && endValue-seqOpts.Increment*cacheSize >= seqOpts.MaxValue) ||
				(seqOpts.Increment < 0 && endValue-seqOpts.Increment*cacheSize <= seqOpts.MinValue) {
				return 0, 0, 0, boundsExceededError(descriptor)
			}
			// Otherwise, values between the limit and the value prior to incrementing can be cached.
			limit := seqOpts.MaxValue
			if seqOpts.Increment < 0 {
				limit = seqOpts.MinValue
			}
			abs := func(i int64) int64 {
				if i < 0 {
					return -i
				}
				return i
			}
			currentValue = endValue - seqOpts.Increment*(cacheSize-1)
			incrementAmount = seqOpts.Increment
			sizeOfCache = abs(limit-(endValue-seqOpts.Increment*cacheSize)) / abs(seqOpts.Increment)
			return currentValue, incrementAmount, sizeOfCache, nil
		}

		return endValue - seqOpts.Increment*(cacheSize-1), seqOpts.Increment, cacheSize, nil
	}

	var val int64
	var err error
	if cacheSize == 1 {
		val, _, _, err = fetchNextValues()
		if err != nil {
			return 0, err
		}
	} else {
		val, err = p.GetOrInitSequenceCache().NextValue(uint32(descriptor.GetID()), uint32(descriptor.GetVersion()), fetchNextValues)
		if err != nil {
			return 0, err
		}
	}
	return val, nil
}

func boundsExceededError(descriptor catalog.TableDescriptor) error {
	seqOpts := descriptor.GetSequenceOpts()
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
	name := descriptor.GetName()
	return pgerror.Newf(
		pgcode.SequenceGeneratorLimitExceeded,
		`reached %s value of sequence %q (%d)`, word,
		tree.ErrString((*tree.Name)(&name)), value)
}

// GetLatestValueInSessionForSequence implements the tree.SequenceOperators interface.
func (p *planner) GetLatestValueInSessionForSequence(
	ctx context.Context, seqName *tree.TableName,
) (int64, error) {
	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc)
	_, descriptor, err := resolver.ResolveExistingTableObject(ctx, p, seqName, flags)
	if err != nil {
		return 0, err
	}
	return getLatestValueInSessionForSequenceHelper(p, descriptor, seqName)
}

// GetLatestValueInSessionForSequenceByID implements the tree.SequenceOperators interface.
func (p *planner) GetLatestValueInSessionForSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc)
	descriptor, err := p.Descriptors().GetImmutableTableByID(ctx, p.txn, descpb.ID(seqID), flags)
	if err != nil {
		return 0, err
	}
	seqName, err := p.getQualifiedTableName(ctx, descriptor)
	if err != nil {
		return 0, err
	}
	if !descriptor.IsSequence() {
		return 0, sqlerrors.NewWrongObjectTypeError(seqName, "sequence")
	}
	return getLatestValueInSessionForSequenceHelper(p, descriptor, seqName)
}

// getLatestValueInSessionForSequenceHelper is shared by
// GetLatestValueInSessionForSequence and GetLatestValueInSessionForSequenceByID
// to get the latest value for the given sequence.
func getLatestValueInSessionForSequenceHelper(
	p *planner, descriptor catalog.TableDescriptor, seqName *tree.TableName,
) (int64, error) {
	val, ok := p.SessionData().SequenceState.GetLastValueByID(uint32(descriptor.GetID()))
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
	_, descriptor, err := resolver.ResolveExistingTableObject(ctx, p, seqName, flags)
	if err != nil {
		return err
	}
	return setSequenceValueHelper(ctx, p, descriptor, newVal, isCalled, seqName)
}

// SetSequenceValueByID implements the tree.SequenceOperators interface.
func (p *planner) SetSequenceValueByID(
	ctx context.Context, seqID int64, newVal int64, isCalled bool,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("setval()")
	}

	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireSequenceDesc)
	descriptor, err := p.Descriptors().GetImmutableTableByID(ctx, p.txn, descpb.ID(seqID), flags)
	if err != nil {
		return err
	}
	seqName, err := p.getQualifiedTableName(ctx, descriptor)
	if err != nil {
		return err
	}
	if !descriptor.IsSequence() {
		return sqlerrors.NewWrongObjectTypeError(seqName, "sequence")
	}
	return setSequenceValueHelper(ctx, p, descriptor, newVal, isCalled, seqName)
}

// setSequenceValueHelper is shared by SetSequenceValue and SetSequenceValueByID
// to set the given sequence to a new given value.
func setSequenceValueHelper(
	ctx context.Context,
	p *planner,
	descriptor catalog.TableDescriptor,
	newVal int64,
	isCalled bool,
	seqName *tree.TableName,
) error {
	if err := p.CheckPrivilege(ctx, descriptor, privilege.UPDATE); err != nil {
		return err
	}

	if descriptor.GetSequenceOpts().Virtual {
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
	ctx context.Context, codec keys.SQLCodec, desc catalog.TableDescriptor,
) (int64, error) {
	if desc.GetSequenceOpts() == nil {
		return 0, errors.New("descriptor is not a sequence")
	}
	keyValue, err := p.txn.Get(ctx, codec.SequenceKey(uint32(desc.GetID())))
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
		// No Caching
		opts.CacheSize = 1
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
				opts.CacheSize = *option.IntVal
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
					return errors.WithHintf(
						pgerror.Newf(pgcode.FeatureNotSupported,
							"OWNED BY cannot refer to other databases; (see the '%s' cluster setting)",
							allowCrossDatabaseSeqOwnerSetting),
						crossDBReferenceDeprecationHint(),
					)
				}
				// We only want to trigger schema changes if the owner is not what we
				// want it to be.
				if opts.SequenceOwner.OwnerTableID != tableDesc.ID ||
					opts.SequenceOwner.OwnerColumnID != col.GetID() {
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
	col, err := tableDesc.FindColumnWithID(opts.SequenceOwner.OwnerColumnID)
	if err != nil {
		return err
	}
	// Find an item in colDesc.OwnsSequenceIds which references SequenceID.
	newOwnsSequenceIDs := make([]descpb.ID, 0, col.NumOwnsSequences())
	for i := 0; i < col.NumOwnsSequences(); i++ {
		id := col.GetOwnsSequenceID(i)
		if id != sequenceID {
			newOwnsSequenceIDs = append(newOwnsSequenceIDs, id)
		}
	}
	if len(newOwnsSequenceIDs) == col.NumOwnsSequences() {
		return errors.AssertionFailedf("couldn't find reference from column to this sequence")
	}
	col.ColumnDesc().OwnsSequenceIds = newOwnsSequenceIDs
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
) (*tabledesc.Mutable, catalog.Column, error) {
	if columnItem.TableName == nil {
		err := pgerror.New(pgcode.Syntax, "invalid OWNED BY option")
		return nil, nil, errors.WithHint(err, "Specify OWNED BY table.column or OWNED BY NONE.")
	}
	tableName := columnItem.TableName.ToTableName()
	_, tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &tableName, true /* required */, tree.ResolveRequireTableDesc)
	if err != nil {
		return nil, nil, err
	}
	col, err := tableDesc.FindColumnWithName(columnItem.ColumnName)
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

	col.ColumnDesc().OwnsSequenceIds = append(col.ColumnDesc().OwnsSequenceIds, sequenceID)

	opts.SequenceOwner.OwnerColumnID = col.GetID()
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
	st *cluster.Settings,
	sc resolver.SchemaResolver,
	tableDesc *tabledesc.Mutable,
	col *descpb.ColumnDescriptor,
	expr tree.TypedExpr,
	backrefs map[descpb.ID]*tabledesc.Mutable,
) ([]*tabledesc.Mutable, error) {
	seqIdentifiers, err := sequence.GetUsedSequences(expr)
	if err != nil {
		return nil, err
	}
	version := st.Version.ActiveVersionOrEmpty(ctx)
	byID := version != (clusterversion.ClusterVersion{}) &&
		version.IsActive(clusterversion.SequencesRegclass)

	var seqDescs []*tabledesc.Mutable
	seqNameToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		seqDesc, err := GetSequenceDescFromIdentifier(ctx, sc, seqIdentifier)
		if err != nil {
			return nil, err
		}
		seqNameToID[seqIdentifier.SeqName] = int64(seqDesc.ID)

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
				ByID:      byID,
			})
		} else {
			seqDesc.DependedOnBy[refIdx].ColumnIDs = append(seqDesc.DependedOnBy[refIdx].ColumnIDs, col.ID)
		}
		seqDescs = append(seqDescs, seqDesc)
	}

	// If sequences are present in the expr (and the cluster is the right version),
	// walk the expr tree and replace any sequences names with their IDs.
	if len(seqIdentifiers) > 0 && byID {
		newExpr, err := sequence.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
		if err != nil {
			return nil, err
		}
		s := tree.Serialize(newExpr)
		col.DefaultExpr = &s
	}

	return seqDescs, nil
}

// GetSequenceDescFromIdentifier resolves the sequence descriptor for the given
// sequence identifier.
func GetSequenceDescFromIdentifier(
	ctx context.Context, sc resolver.SchemaResolver, seqIdentifier sequence.SeqIdentifier,
) (*tabledesc.Mutable, error) {
	var tn tree.TableName
	if seqIdentifier.IsByID() {
		name, err := sc.GetQualifiedTableNameByID(ctx, seqIdentifier.SeqID, tree.ResolveRequireSequenceDesc)
		if err != nil {
			return nil, err
		}
		tn = *name
	} else {
		parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
		if err != nil {
			return nil, err
		}
		tn = parsedSeqName.ToTableName()
	}

	var seqDesc *tabledesc.Mutable
	var err error
	p, ok := sc.(*planner)
	if ok {
		_, seqDesc, err = p.ResolveMutableTableDescriptor(ctx, &tn, true /*required*/, tree.ResolveRequireSequenceDesc)
		if err != nil {
			return nil, err
		}
	} else {
		// This is only executed via IMPORT which uses its own resolver.
		_, seqDesc, err = resolver.ResolveMutableExistingTableObject(ctx, sc, &tn, true /*required*/, tree.ResolveRequireSequenceDesc)
		if err != nil {
			return nil, err
		}
	}
	return seqDesc, nil
}

// dropSequencesOwnedByCol drops all the sequences from col.OwnsSequenceIDs.
// Called when the respective column (or the whole table) is being dropped.
func (p *planner) dropSequencesOwnedByCol(
	ctx context.Context, col catalog.Column, queueJob bool, behavior tree.DropBehavior,
) error {
	// Copy out the sequence IDs as the code to drop the sequence will reach
	// back around and update the descriptor from underneath us.
	colOwnsSequenceIDs := make([]descpb.ID, col.NumOwnsSequences())
	for i := 0; i < col.NumOwnsSequences(); i++ {
		colOwnsSequenceIDs[i] = col.GetOwnsSequenceID(i)
	}

	for _, sequenceID := range colOwnsSequenceIDs {
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
			ctx, seqDesc, queueJob, jobDesc, behavior,
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
	ctx context.Context, tableDesc *tabledesc.Mutable, col catalog.Column,
) error {
	for i := 0; i < col.NumUsesSequences(); i++ {
		sequenceID := col.GetUsesSequenceID(i)
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
					if colRefID == col.GetID() {
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
	col.ColumnDesc().UsesSequenceIds = []descpb.ID{}
	return nil
}
