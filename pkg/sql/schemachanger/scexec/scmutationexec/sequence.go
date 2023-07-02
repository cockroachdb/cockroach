// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"
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
		Privileges:    &catpb.PrivilegeDescriptor{Version: catpb.Version21_2}, // Populated by `UserPrivileges` elements and `Owner` element
		Version:       1,
		FormatVersion: descpb.InterleavedFormatVersion,
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

func (i *immediateVisitor) SetSequenceOptions(
	ctx context.Context, op scop.SetSequenceOptions,
) error {
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
		tree.SeqOptIncrement: {SetFunc: setIntValue(&sc.SequenceOpts.Increment)},
		tree.SeqOptMinValue:  {SetFunc: setIntValue(&sc.SequenceOpts.MinValue)},
		tree.SeqOptMaxValue:  {SetFunc: setIntValue(&sc.SequenceOpts.MaxValue)},
		tree.SeqOptStart:     {SetFunc: setIntValue(&sc.SequenceOpts.Start)},
		tree.SeqOptCache:     {SetFunc: setIntValue(&sc.SequenceOpts.CacheSize)},
		tree.SeqOptVirtual:   {SetFunc: setBoolValue(&sc.SequenceOpts.Virtual)},
		tree.SeqOptAs: {SetFunc: func(Value string) error {
			sc.SequenceOpts.AsIntegerType = Value
			return nil
		}},
	}
	return sequenceOptionMeta[op.Key].SetFunc(op.Value)
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
