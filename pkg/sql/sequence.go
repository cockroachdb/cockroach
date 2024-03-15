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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// GetSerialSequenceNameFromColumn is part of the eval.SequenceOperators interface.
func (p *planner) GetSerialSequenceNameFromColumn(
	ctx context.Context, tn *tree.TableName, columnName tree.Name,
) (*tree.TableName, error) {
	flags := tree.ObjectLookupFlags{
		Required:             true,
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: tree.ResolveRequireTableDesc,
	}
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
				seq, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, col.GetUsesSequenceID(0))
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

// IncrementSequenceByID implements the eval.SequenceOperators interface.
func (p *planner) IncrementSequenceByID(ctx context.Context, seqID int64) (int64, error) {
	if p.EvalContext().TxnReadOnly {
		return 0, readOnlyError("nextval()")
	}
	descriptor, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(seqID))
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

	requiredPrivileges := []privilege.Kind{privilege.USAGE, privilege.UPDATE}
	hasRequiredPriviledge := false

	for _, priv := range requiredPrivileges {
		err := p.CheckPrivilege(ctx, descriptor, priv)
		if err == nil {
			hasRequiredPriviledge = true
			break
		}
	}
	if !hasRequiredPriviledge {
		return 0, sqlerrors.NewInsufficientPrivilegeOnDescriptorError(p.User(), requiredPrivileges,
			string(descriptor.DescriptorType()), descriptor.GetName())
	}

	var err error
	seqOpts := descriptor.GetSequenceOpts()

	var val int64
	if seqOpts.Virtual {
		rowid := builtins.GenerateUniqueInt(
			builtins.ProcessUniqueID(p.EvalContext().NodeID.SQLInstanceID()),
		)
		val = int64(rowid)
	} else {
		val, err = p.incrementSequenceUsingCache(ctx, descriptor)
	}
	if err != nil {
		return 0, err
	}

	p.sessionDataMutatorIterator.applyOnEachMutator(
		func(m sessionDataMutator) {
			m.RecordLatestSequenceVal(uint32(descriptor.GetID()), val)
		},
	)

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

	sequenceID := descriptor.GetID()
	createdInCurrentTxn := p.createdSequences.isCreatedSequence(sequenceID)
	var cacheSize int64
	if createdInCurrentTxn {
		cacheSize = 1
	} else {
		cacheSize = seqOpts.EffectiveCacheSize()
	}

	fetchNextValues := func() (nextValue, incrementAmount, sizeOfCache int64, err error) {
		seqValueKey := p.ExecCfg().Codec.SequenceKey(uint32(sequenceID))
		sizeOfCache = cacheSize
		incrementAmount = seqOpts.Increment
		var res kv.KeyValue
		var currentValue int64
		var endValue int64

		var getSequenceValueFunc func() (kv.KeyValue, error)
		var cputSequenceValueFunc func() error
		if createdInCurrentTxn {
			// The planner txn is only used if the sequence is accessed in the same
			// transaction that it was created. Otherwise, we *do not* use the planner
			// txn here, since nextval does not respect transaction boundaries.
			// This matches the specification at
			// https://www.postgresql.org/docs/14/functions-sequence.html.
			getSequenceValueFunc = func() (kv.KeyValue, error) {
				return p.txn.Get(ctx, seqValueKey)
			}
			cputSequenceValueFunc = func() error {
				return p.txn.CPut(ctx, seqValueKey, endValue, res.Value.TagAndDataBytes())
			}
		} else {
			getSequenceValueFunc = func() (kv.KeyValue, error) {
				return p.ExecCfg().DB.Get(ctx, seqValueKey)
			}
			cputSequenceValueFunc = func() error {
				return p.ExecCfg().DB.CPut(ctx, seqValueKey, endValue, res.Value.TagAndDataBytes())
			}
		}

		// Get the current value of the sequence.
		res, err = getSequenceValueFunc()
		if err != nil {
			return 0, 0, 0, err
		}
		currentValue = res.ValueInt()
		endValue = currentValue + incrementAmount*sizeOfCache

		// If the endValue is outside the limits of the sequence,
		// the cache will only increment upto the limit.
		if endValue > seqOpts.MaxValue || endValue < seqOpts.MinValue {
			limit := seqOpts.MaxValue
			if incrementAmount < 0 {
				limit = seqOpts.MinValue
			}
			abs := func(i int64) int64 {
				if i < 0 {
					return -i
				}
				return i
			}
			// Calculate the size of the cache the last value before the limit.
			sizeOfCache = abs((limit - currentValue) / incrementAmount)
			endValue = currentValue + incrementAmount*(sizeOfCache)
			// If sizeOfCache is zero, the sequence is already out of bounds.
			if sizeOfCache == 0 {
				return 0, 0, 0, boundsExceededError(descriptor)
			}
		}

		// Write increased sequence value with CPut ensuring value consistency between calculations.
		err = cputSequenceValueFunc()
		if err != nil {
			if errors.HasType(err, (*kvpb.IntegerOverflowError)(nil)) {
				return 0, 0, 0, boundsExceededError(descriptor)
			}
			return 0, 0, 0, err
		}

		nextValue = currentValue + incrementAmount
		return nextValue, incrementAmount, sizeOfCache, nil
	}

	// Wrap fetchNextValues with a retry function if cput fails to match the expected value.
	// This can happen if multiple users or transcations update the sequence at once.
	fetchNextValuesRetry := func() (nextValue, incrementAmount, sizeOfCache int64, err error) {
		for r := retry.Start(base.DefaultRetryOptions()); r.Next(); {
			nextValue, incrementAmount, sizeOfCache, err = fetchNextValues()
			if errors.HasType(err, (*kvpb.ConditionFailedError)(nil)) {
				continue
			}
			break
		}
		return nextValue, incrementAmount, sizeOfCache, err
	}

	var val int64
	var err error
	if cacheSize == 1 {
		val, _, _, err = fetchNextValuesRetry()
		if err != nil {
			return 0, err
		}
	} else {
		// If cache size option is 1 (default -> not cached), and node cache size option is not 0 (not default -> node-cached), then use node-level cache
		if seqOpts.CacheSize == 1 && seqOpts.NodeCacheSize != 0 {
			val, err = p.GetSequenceCacheNode().NextValue(sequenceID, uint32(descriptor.GetVersion()), fetchNextValuesRetry)
		} else {
			val, err = p.GetOrInitSequenceCache().NextValue(uint32(sequenceID), uint32(descriptor.GetVersion()), fetchNextValuesRetry)
		}
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

// GetLatestValueInSessionForSequenceByID implements the eval.SequenceOperators interface.
func (p *planner) GetLatestValueInSessionForSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	descriptor, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(seqID))
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

// SetSequenceValueByID implements the eval.SequenceOperators interface.
func (p *planner) SetSequenceValueByID(
	ctx context.Context, seqID uint32, newVal int64, isCalled bool,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("setval()")
	}

	descriptor, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(seqID))
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

	createdInCurrentTxn := p.createdSequences.isCreatedSequence(descriptor.GetID())
	if createdInCurrentTxn {
		// The planner txn is only used if the sequence is accessed in the same
		// transaction that it was created or restarted.
		if err := p.txn.Put(ctx, seqValueKey, newVal); err != nil {
			return err
		}
	} else {
		// Otherwise, we *do not* use the planner txn here, since setval does not
		// respect transaction boundaries. This matches the specification at
		// https://www.postgresql.org/docs/14/functions-sequence.html.
		// TODO(vilterp): not supposed to mix usage of Inc and Put on a key,
		// according to comments on Inc operation. Switch to Inc if `desired-current`
		// overflows correctly.
		if err := p.ExecCfg().DB.Put(ctx, seqValueKey, newVal); err != nil {
			return err
		}
	}

	// Clear out the cache and update the last value if needed.
	p.sessionDataMutatorIterator.applyOnEachMutator(func(m sessionDataMutator) {
		m.initSequenceCache()
		if isCalled {
			m.RecordLatestSequenceVal(seqID, newVal)
		}
	})
	return nil
}

// GetLastSequenceValueByID implements the eval.SequenceOperators interface.
func (p *planner) GetLastSequenceValueByID(
	ctx context.Context, seqID uint32,
) (val int64, wasCalled bool, err error) {
	descriptor, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(seqID))
	if err != nil {
		return 0, false, err
	}
	seqName, err := p.getQualifiedTableName(ctx, descriptor)
	if err != nil {
		return 0, false, err
	}
	if !descriptor.IsSequence() {
		return 0, false, sqlerrors.NewWrongObjectTypeError(seqName, "sequence")
	}
	val, err = getSequenceValueFromDesc(ctx, p.txn, p.execCfg.Codec, descriptor)
	if err != nil {
		return 0, false, err
	}

	// Before using for the first time, sequenceValue will be:
	// opts.Start - opts.Increment.
	opts := descriptor.GetSequenceOpts()
	return val, val != opts.Start-opts.Increment, nil
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
	if !desc.IsSequence() {
		return 0, errors.New("descriptor is not a sequence")
	}
	return getSequenceValueFromDesc(ctx, p.txn, codec, desc)
}

func getSequenceValueFromDesc(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, desc catalog.TableDescriptor,
) (int64, error) {
	keyValue, err := txn.Get(ctx, codec.SequenceKey(uint32(desc.GetID())))
	if err != nil {
		return 0, err
	}
	return keyValue.ValueInt(), nil
}

func readOnlyError(s string) error {
	return pgerror.Newf(pgcode.ReadOnlySQLTransaction,
		"cannot execute %s in a read-only transaction", s)
}

func assignSequenceOwner(
	ctx context.Context,
	p *planner,
	opts *descpb.TableDescriptor_SequenceOpts,
	optsNode tree.SequenceOptions,
	sequenceID descpb.ID,
	sequenceParentID descpb.ID,
) error {
	optionsSeen := map[string]bool{}
	for _, option := range optsNode {
		// Error on duplicate options.
		_, seenBefore := optionsSeen[option.Name]
		if seenBefore {
			return pgerror.New(pgcode.Syntax, "conflicting or redundant options")
		}
		optionsSeen[option.Name] = true
		switch option.Name {
		case tree.SeqOptOwnedBy:
			if p == nil {
				return pgerror.Newf(pgcode.Internal,
					"Trying to add/remove Sequence Owner outside of context of a planner")
			}
			// The owner is being removed
			if option.ColumnItemVal == nil {
				if err := removeSequenceOwnerIfExists(ctx, p, sequenceID, opts); err != nil {
					return err
				}
			} else {
				// The owner is being added/modified
				tableDesc, col, err := resolveColumnItemToDescriptors(
					ctx, p, option.ColumnItemVal,
				)
				if err != nil {
					return err
				}
				if tableDesc.ParentID != sequenceParentID {
					if err := p.CanCreateCrossDBSequenceOwnerRef(); err != nil {
						return err
					}
				}
				// We only want to trigger schema changes if the owner is not what we
				// want it to be.
				if opts.SequenceOwner.OwnerTableID != tableDesc.ID ||
					opts.SequenceOwner.OwnerColumnID != col.GetID() {
					if err := removeSequenceOwnerIfExists(ctx, p, sequenceID, opts); err != nil {
						return err
					}
					err := addSequenceOwner(ctx, p, option.ColumnItemVal, sequenceID, opts)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// checkDupSeqOption check if there is any duplicate sequence option.
func checkDupSeqOption(optsNode tree.SequenceOptions) error {
	optionsSeen := map[string]bool{}
	for _, option := range optsNode {
		// Error on duplicate options.
		_, seenBefore := optionsSeen[option.Name]
		if seenBefore {
			return pgerror.New(pgcode.Syntax, "conflicting or redundant options")
		}
		optionsSeen[option.Name] = true
	}
	return nil
}

// assignSequenceOptions moves options from the AST node to the sequence options descriptor,
// starting with defaults and overriding them with user-provided options.
func assignSequenceOptions(
	ctx context.Context,
	p *planner,
	opts *descpb.TableDescriptor_SequenceOpts,
	optsNode tree.SequenceOptions,
	setDefaults bool,
	sequenceID descpb.ID,
	sequenceParentID descpb.ID,
	existingType *types.T,
) error {
	if err := checkDupSeqOption(optsNode); err != nil {
		return err
	}

	defaultIntSize := int32(64)
	if p != nil && p.SessionData() != nil {
		defaultIntSize = p.SessionData().DefaultIntSize
	}
	if err := schemaexpr.AssignSequenceOptions(
		opts,
		optsNode,
		defaultIntSize,
		setDefaults,
		existingType,
	); err != nil {
		return pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
	}
	if err := assignSequenceOwner(
		ctx,
		p,
		opts,
		optsNode,
		sequenceID,
		sequenceParentID,
	); err != nil {
		return pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
	}
	return nil
}

func removeSequenceOwnerIfExists(
	ctx context.Context, p *planner, sequenceID descpb.ID, opts *descpb.TableDescriptor_SequenceOpts,
) error {
	if !opts.HasOwner() {
		return nil
	}
	tableDesc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, opts.SequenceOwner.OwnerTableID)
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
	col, err := catalog.MustFindColumnByID(tableDesc, opts.SequenceOwner.OwnerColumnID)
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
	col, err := catalog.MustFindColumnByTreeName(tableDesc, columnItem.ColumnName)
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
// `colExprKind`, either 'DEFAULT' or "ON UPDATE", tells which expression `expr` is, so we can
// correctly modify `col` (see issue #81333).
func maybeAddSequenceDependencies(
	ctx context.Context,
	st *cluster.Settings,
	sc resolver.SchemaResolver,
	tableDesc catalog.TableDescriptor,
	col *descpb.ColumnDescriptor,
	expr tree.TypedExpr,
	backrefs map[descpb.ID]*tabledesc.Mutable,
	colExprKind tabledesc.ColExprKind,
) ([]*tabledesc.Mutable, error) {
	seqIdentifiers, err := seqexpr.GetUsedSequences(expr)
	if err != nil {
		return nil, err
	}

	var seqDescs []*tabledesc.Mutable
	seqNameToID := make(map[string]descpb.ID)
	for _, seqIdentifier := range seqIdentifiers {
		seqDesc, err := GetSequenceDescFromIdentifier(ctx, sc, seqIdentifier)
		if err != nil {
			return nil, err
		}
		// Check if this reference is cross DB.
		if seqDesc.GetParentID() != tableDesc.GetParentID() &&
			!allowCrossDatabaseSeqReferences.Get(&st.SV) {
			return nil, errors.WithHintf(
				pgerror.Newf(pgcode.FeatureNotSupported,
					"sequence references cannot come from other databases; (see the '%s' cluster setting)",
					allowCrossDatabaseSeqReferencesSetting),
				crossDBReferenceDeprecationHint(),
			)

		}
		seqNameToID[seqIdentifier.SeqName] = seqDesc.ID

		// If we had already modified this Sequence as part of this transaction,
		// we only want to modify a single instance of it instead of overwriting it.
		// So replace seqDesc with the descriptor that was previously modified.
		if prev, ok := backrefs[seqDesc.ID]; ok {
			seqDesc = prev
		}
		// Add reference from sequence descriptor to column.
		{
			var found bool
			for _, seqID := range col.UsesSequenceIds {
				if seqID == seqDesc.ID {
					found = true
					break
				}
			}
			if !found {
				col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.ID)
			}
		}
		refIdx := -1
		for i, reference := range seqDesc.DependedOnBy {
			if reference.ID == tableDesc.GetID() {
				refIdx = i
			}
		}
		if refIdx == -1 {
			seqDesc.DependedOnBy = append(seqDesc.DependedOnBy, descpb.TableDescriptor_Reference{
				ID:        tableDesc.GetID(),
				ColumnIDs: []descpb.ColumnID{col.ID},
				ByID:      true,
			})
		} else {
			ref := &seqDesc.DependedOnBy[refIdx]
			var found bool
			for _, colID := range ref.ColumnIDs {
				if colID == col.ID {
					found = true
					break
				}
			}
			if !found {
				ref.ColumnIDs = append(ref.ColumnIDs, col.ID)
			}
		}
		seqDescs = append(seqDescs, seqDesc)
	}

	// If sequences are present in the expr (and the cluster is the right version),
	// walk the expr tree and replace any sequences names with their IDs.
	if len(seqIdentifiers) > 0 {
		newExpr, err := seqexpr.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
		if err != nil {
			return nil, err
		}
		s := tree.Serialize(newExpr)
		switch colExprKind {
		case tabledesc.DefaultExpr:
			col.DefaultExpr = &s
		case tabledesc.OnUpdateExpr:
			col.OnUpdateExpr = &s
		default:
			return nil, errors.AssertionFailedf("colExprKind must be either 'DEFAULT' or 'ON UPDATE'; got %v", colExprKind)
		}
	}

	return seqDescs, nil
}

// GetSequenceDescFromIdentifier resolves the sequence descriptor for the given
// sequence identifier.
func GetSequenceDescFromIdentifier(
	ctx context.Context, sc resolver.SchemaResolver, seqIdentifier seqexpr.SeqIdentifier,
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
		seqDesc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, sequenceID)
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
//
// The column descriptor is mutated but not saved to persistent storage; the caller must save it.
func (p *planner) removeSequenceDependencies(
	ctx context.Context, tableDesc *tabledesc.Mutable, col catalog.Column,
) error {
	for i := 0; i < col.NumUsesSequences(); i++ {
		sequenceID := col.GetUsesSequenceID(i)
		// Get the sequence descriptor so we can remove the reference from it.
		seqDesc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, sequenceID)
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
