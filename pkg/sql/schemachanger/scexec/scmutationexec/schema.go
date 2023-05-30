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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

func (i *immediateVisitor) CreateSchemaDescriptor(
	ctx context.Context, op scop.CreateSchemaDescriptor,
) error {
	mut := schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ParentID:   catid.InvalidDescID, // Set by `SchemaParent` element
		Name:       "",                  // Set by `Namespace` element
		ID:         op.SchemaID,
		Privileges: &catpb.PrivilegeDescriptor{Version: catpb.Version21_2}, // Populated by `UserPrivileges` elements and `Owner` element
		Version:    1,
	}).BuildCreatedMutableSchema()
	mut.State = descpb.DescriptorState_ADD
	i.CreateDescriptor(mut)
	return nil
}
