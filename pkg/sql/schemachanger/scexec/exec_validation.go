// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func executeValidationOps(ctx context.Context, deps Dependencies, execute []scop.Op) error {
	for _, op := range execute {
		switch op := op.(type) {
		case *scop.ValidateUniqueIndex:
			desc, err := deps.Catalog().MustReadImmutableDescriptor(ctx, op.TableID)
			if err != nil {
				return err
			}
			table, ok := desc.(catalog.TableDescriptor)
			if !ok {
				return catalog.WrapTableDescRefErr(desc.GetID(), catalog.NewDescriptorTypeError(desc))
			}
			index, err := table.FindIndexWithID(op.IndexID)
			if err != nil {
				return err
			}
			// Execute the validation operation as a root user.
			execOverride := sessiondata.InternalExecutorOverride{
				User: security.RootUserName(),
			}
			if index.GetType() == descpb.IndexDescriptor_FORWARD {
				err = deps.IndexValidator().ValidateForwardIndexes(ctx, table, []catalog.Index{index}, true, false, execOverride)
			} else {
				err = deps.IndexValidator().ValidateInvertedIndexes(ctx, table, []catalog.Index{index}, false, execOverride)
			}
			return err
		case *scop.ValidateCheckConstraint:
			log.Errorf(ctx, "not implemented")
		default:
			panic("unimplemented")
		}
	}
	return nil
}
