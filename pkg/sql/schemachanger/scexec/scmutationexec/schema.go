// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

func (i *immediateVisitor) CreateSchemaDescriptor(
	ctx context.Context, op scop.CreateSchemaDescriptor,
) error {
	mut := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ParentID:   catid.InvalidDescID, // Set by `SchemaParent` element
		Name:       "",                  // Set by `SchemaName` element
		ID:         op.SchemaID,
		Privileges: &catpb.PrivilegeDescriptor{Version: catpb.Version23_2}, // Populated by `UserPrivileges` elements and `Owner` element
		Version:    1,
		State:      descpb.DescriptorState_ADD,
	}).BuildCreatedMutableSchema()
	i.CreateDescriptor(mut)
	return nil
}
