// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (i *immediateVisitor) CreateSequenceDescriptor(
	_ context.Context, op scop.CreateSequenceDescriptor,
) error {
	mut := tabledesc.NewBuilder(&descpb.TableDescriptor{
		ParentID:      catid.InvalidDescID, // Set by `SchemaParent` element
		Name:          "",                  // Set by `Namespace` element
		ID:            op.SequenceID,
		Privileges:    &catpb.PrivilegeDescriptor{Version: catpb.Version23_2}, // Populated by `UserPrivileges` elements and `Owner` element
		Version:       1,
		FormatVersion: descpb.InterleavedFormatVersion,
		Temporary:     op.Temporary,
	}).BuildCreatedMutable()
	tabledDesc := mut.(*tabledesc.Mutable)
	tabledDesc.State = descpb.DescriptorState_ADD
	// Set the default sequence options.
	tabledDesc.SequenceOpts = &descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	if err := schemaexpr.AssignSequenceOptions(tabledDesc.SequenceOpts,
		nil,
		64,
		true,
		nil,
	); err != nil {
		return err
	}
	i.CreateDescriptor(mut)
	return nil
}

// SetSequenceOption sets a sequence option to the provided value. It updates
// the current value of the sequence on restart.
func (i *immediateVisitor) SetSequenceOption(ctx context.Context, op scop.SetSequenceOption) error {
	sc, err := i.checkOutTable(ctx, op.SequenceID)
	if err != nil {
		return err
	}

	setIntValue := func(target *int64) func(Value string) error {
		return func(Value string) error {
			var err error
			*target, err = strconv.ParseInt(Value, 10, 64)
			return err
		}
	}
	setBoolValue := func(target *bool) func(Value string) error {
		return func(Value string) error {
			var err error
			*target, err = strconv.ParseBool(Value)
			return err
		}
	}
	sequenceOptionMeta := map[string]struct {
		SetFunc func(Value string) error
	}{
		tree.SeqOptIncrement:    {SetFunc: setIntValue(&sc.SequenceOpts.Increment)},
		tree.SeqOptMinValue:     {SetFunc: setIntValue(&sc.SequenceOpts.MinValue)},
		tree.SeqOptMaxValue:     {SetFunc: setIntValue(&sc.SequenceOpts.MaxValue)},
		tree.SeqOptStart:        {SetFunc: setIntValue(&sc.SequenceOpts.Start)},
		tree.SeqOptCacheSession: {SetFunc: setIntValue(&sc.SequenceOpts.SessionCacheSize)},
		tree.SeqOptCacheNode:    {SetFunc: setIntValue(&sc.SequenceOpts.NodeCacheSize)},
		tree.SeqOptVirtual:      {SetFunc: setBoolValue(&sc.SequenceOpts.Virtual)},
		tree.SeqOptAs: {SetFunc: func(Value string) error {
			sc.SequenceOpts.AsIntegerType = Value
			return nil
		}},
	}

	switch key := op.Key; key {
	case tree.SeqOptRestart:
		restartWith, err := strconv.ParseInt(op.Value, 10, 64)
		if err != nil {
			return err
		}
		i.ImmediateMutationStateUpdater.SetSequence(op.SequenceID, restartWith)
		return nil
	}

	return sequenceOptionMeta[op.Key].SetFunc(op.Value)
}

// UnsetSequenceOption sets a sequence option to its default.
func (i *immediateVisitor) UnsetSequenceOption(
	ctx context.Context, op scop.UnsetSequenceOption,
) error {
	defaultOpts := schemaexpr.DefaultSequenceOptions()

	setOp := scop.SetSequenceOption{
		SequenceID: op.SequenceID,
		Key:        op.Key,
	}

	switch op.Key {

	case tree.SeqOptAs:
		setOp.Value = defaultOpts.AsIntegerType
	case tree.SeqOptCacheNode:
		setOp.Value = fmt.Sprintf("%d", defaultOpts.NodeCacheSize)
	case tree.SeqOptCacheSession:
		setOp.Value = fmt.Sprintf("%d", defaultOpts.SessionCacheSize)
	case tree.SeqOptIncrement:
		setOp.Value = fmt.Sprintf("%d", defaultOpts.Increment)
	case tree.SeqOptMinValue:
		setOp.Value = fmt.Sprintf("%d", defaultOpts.MinValue)
	case tree.SeqOptMaxValue:
		setOp.Value = fmt.Sprintf("%d", defaultOpts.MaxValue)
	case tree.SeqOptStart:
		setOp.Value = fmt.Sprintf("%d", defaultOpts.Start)
	case tree.SeqOptRestart:
		// Noop on unsetting a transient element.
		return nil
	case tree.SeqOptVirtual:
		setOp.Value = fmt.Sprintf("%t", defaultOpts.Virtual)
	default:
		panic(fmt.Sprintf("unexpected sequence option: %s", op.Key))
	}

	return i.SetSequenceOption(ctx, setOp)
}

// MaybeUpdateSequenceValue updates the value of the sequence when changes to
// the sequence options demand it. It is best effort.
func (i *immediateVisitor) MaybeUpdateSequenceValue(
	ctx context.Context, op scop.MaybeUpdateSequenceValue,
) error {
	_, err := i.checkOutTable(ctx, op.SequenceID)
	if err != nil {
		return err
	}

	i.ImmediateMutationStateUpdater.MaybeUpdateSequenceValue(op.SequenceID, op)

	return nil
}

func (i *immediateVisitor) InitSequence(ctx context.Context, op scop.InitSequence) error {
	sc, err := i.checkOutTable(ctx, op.SequenceID)
	if err != nil {
		return err
	}
	startVal := sc.SequenceOpts.Start
	if op.UseRestartWith {
		startVal = op.RestartWith
	}
	startVal = startVal - sc.SequenceOpts.Increment
	i.ImmediateMutationStateUpdater.InitSequence(sc.ID, startVal)
	return nil
}
