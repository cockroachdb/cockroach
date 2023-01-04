// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ErrMutableTableImplicitType indicates that a table implicit type was fetched
// as a mutable, which is not allowed.
var ErrMutableTableImplicitType = pgerror.Newf(pgcode.DependentObjectsStillExist, "table implicit type not mutable")

// GetMutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetMutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, objectFlags tree.ObjectLookupFlags,
) (found bool, _ *typedesc.Mutable, _ error) {
	b := tc.ByName(txn).WithObjFlags(objectFlags)
	p, err := getObjectPrefix(ctx, b.Immutable(), name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return false, nil, err
	}
	typ, err := b.Mutable().Type(ctx, p.Database, p.Schema, name.Object())
	return typ != nil, typ, err
}

// GetImmutableTypeByName returns a mutable type descriptor with properties
// according to the provided lookup flags. RequireMutable is ignored.
func (tc *Collection) GetImmutableTypeByName(
	ctx context.Context, txn *kv.Txn, name tree.ObjectName, objectFlags tree.ObjectLookupFlags,
) (found bool, _ catalog.TypeDescriptor, _ error) {
	g := tc.ByName(txn).WithObjFlags(objectFlags).Immutable()
	p, err := getObjectPrefix(ctx, g, name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return false, nil, err
	}
	typ, err := g.Type(ctx, p.Database, p.Schema, name.Object())
	return typ != nil, typ, err
}
