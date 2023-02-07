// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

func (p *planner) updateFunctionReferencesForCheck(
	ctx context.Context, tblDesc catalog.TableDescriptor, ck *descpb.TableDescriptor_CheckConstraint,
) error {
	udfIDs, err := tblDesc.GetAllReferencedFunctionIDsInConstraint(ck.ConstraintID)
	if err != nil {
		return err
	}
	for _, id := range udfIDs.Ordered() {
		fnDesc, err := p.descCollection.MutableByID(p.txn).Function(ctx, id)
		if err != nil {
			return err
		}
		if err := fnDesc.AddConstraintReference(tblDesc.GetID(), ck.ConstraintID); err != nil {
			return err
		}
		if err := p.writeFuncSchemaChange(ctx, fnDesc); err != nil {
			return err
		}
	}
	return nil
}
