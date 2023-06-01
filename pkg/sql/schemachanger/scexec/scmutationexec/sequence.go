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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
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
	tabledDesc.SequenceOpts = &descpb.TableDescriptor_SequenceOpts{}
	i.CreateDescriptor(mut)
	return nil
}

func (i *immediateVisitor) CreateOrUpdateSequenceOptions(
	ctx context.Context, op scop.CreateOrUpdateSequenceOptions,
) error {
	sc, err := i.checkOutTable(ctx, op.SequenceID)
	if err != nil {
		return err
	}
	sc.SequenceOpts.Increment = op.Increment
	sc.SequenceOpts.MinValue = op.Min
	sc.SequenceOpts.MaxValue = op.Max
	sc.SequenceOpts.Start = op.Start
	sc.SequenceOpts.CacheSize = op.CacheSize
	sc.SequenceOpts.Virtual = op.Virtual
	sc.SequenceOpts.AsIntegerType = op.AsIntegerType
	return nil
}

func (i *immediateVisitor) InitSequence(ctx context.Context, op scop.InitSequence) error {
	sc, err := i.checkOutTable(ctx, op.SequenceID)
	if err != nil {
		return err
	}
	startVal := sc.SequenceOpts.Start
	if op.RestartWith != nil {
		startVal = *op.RestartWith
	}
	startVal = startVal - sc.SequenceOpts.Increment
	i.ImmediateMutationStateUpdater.InitSequence(sc.ID, startVal)
	return nil
}
