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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (i *immediateVisitor) MarkDescriptorAsPublic(
	ctx context.Context, op scop.MarkDescriptorAsPublic,
) error {
	desc, err := i.checkOutDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	desc.SetPublic()
	return nil
}

func (i *immediateVisitor) AddDescriptorName(ctx context.Context, op scop.AddDescriptorName) error {
	nameDetails := descpb.NameInfo{
		ParentID:       op.Namespace.DatabaseID,
		ParentSchemaID: op.Namespace.SchemaID,
		Name:           op.Namespace.Name,
	}
	i.AddName(op.Namespace.DescriptorID, nameDetails)
	desc, err := i.checkOutDescriptor(ctx, op.Namespace.DescriptorID)
	if err != nil {
		return err
	}

	switch t := desc.(type) {
	case *tabledesc.Mutable:
		t.ParentID = op.Namespace.DatabaseID
		t.UnexposedParentSchemaID = op.Namespace.SchemaID
		t.SetName(op.Namespace.Name)
	}
	return nil
}
