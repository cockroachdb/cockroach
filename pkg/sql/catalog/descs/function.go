// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// GetMutableFunctionByID returns a mutable function descriptor.
func (tc *Collection) GetMutableFunctionByID(
	ctx context.Context, txn *kv.Txn, fnID descpb.ID, flags tree.ObjectLookupFlags,
) (*funcdesc.Mutable, error) {
	return tc.ByID(txn).WithObjFlags(flags).Mutable().Function(ctx, fnID)
}
