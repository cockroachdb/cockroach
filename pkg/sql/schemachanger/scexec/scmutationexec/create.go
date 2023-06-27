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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
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
	}
	return nil
}

func (i *immediateVisitor) SetNameInDescriptor(
	ctx context.Context, op scop.SetNameInDescriptor,
) error {
	mut, err := i.checkOutDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	switch mut.DescriptorType() {
	case catalog.Database:
		mut.(*dbdesc.Mutable).Name = op.Name
	case catalog.Schema:
		mut.(*schemadesc.Mutable).Name = op.Name
	case catalog.Table:
		mut.(*tabledesc.Mutable).Name = op.Name
	case catalog.Type:
		mut.(*typedesc.Mutable).Name = op.Name
	case catalog.Function, catalog.Any:
		// functions do not have a namespace entry and their name field is handled
		// by FunctionName element.
		return errors.AssertionFailedf("Incorrect descriptor type %v", mut.DescriptorType())
	}
	return nil
}
