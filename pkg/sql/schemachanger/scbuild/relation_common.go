// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (b *buildContext) removeTypeBackRefDeps(
	ctx context.Context, tableDesc catalog.TableDescriptor,
) {
	// TODO(fqazi):  Consider cleaning up all references by getting them using tableDesc.GetReferencedDescIDs(),
	// which would include all types of references inside a table descriptor. However, this would also need us
	// to look up the type of descriptor.
	_, dbDesc, err := b.Descs.GetImmutableDatabaseByID(ctx, b.EvalCtx.Txn,
		tableDesc.GetParentID(), tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		panic(err)
	}
	typeIDs, err := tableDesc.GetAllReferencedTypeIDs(dbDesc, func(id descpb.ID) (catalog.TypeDescriptor, error) {
		mutDesc, err := b.Descs.GetMutableTypeByID(ctx, b.EvalCtx.Txn, id, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return nil, err
		}
		return mutDesc, nil
	})
	if err != nil {
		panic(err)
	}
	// Drop all references to this table/view/sequence
	for _, typeID := range typeIDs {
		b.addNode(scpb.Target_DROP,
			&scpb.TypeReference{
				TypeID: typeID,
				DescID: tableDesc.GetID(),
			})
	}
}
