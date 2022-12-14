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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// GetDescriptorCollidingWithObjectName returns the descriptor which collides
// with the desired name if it exists.
func GetDescriptorCollidingWithObjectName(
	ctx context.Context, tc *Collection, txn *kv.Txn, parentID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, error) {
	id, err := tc.LookupObjectID(ctx, txn, parentID, parentSchemaID, name)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	// At this point the ID is already in use by another object.
	flags := tree.CommonLookupFlags{
		AvoidLeased:    true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	desc, err := tc.getDescriptorByID(ctx, txn, flags, id)
	if errors.Is(err, catalog.ErrDescriptorNotFound) {
		// Since the ID exists the descriptor should absolutely exist.
		err = errors.NewAssertionErrorWithWrappedErrf(err,
			"parentID=%d parentSchemaID=%d name=%q has ID=%d",
			parentID, parentSchemaID, name, id)
	}
	return desc, err
}

// CheckObjectNameCollision returns an error if the object name is already used.
func CheckObjectNameCollision(
	ctx context.Context,
	tc *Collection,
	txn *kv.Txn,
	parentID, parentSchemaID descpb.ID,
	name tree.ObjectName,
) error {
	d, err := GetDescriptorCollidingWithObjectName(ctx, tc, txn, parentID, parentSchemaID, name.Object())
	if err != nil || d == nil {
		return err
	}
	maybeQualifiedName := name.Object()
	if name.Catalog() != "" && name.Schema() != "" {
		maybeQualifiedName = name.FQString()
	}
	return sqlerrors.MakeObjectAlreadyExistsError(d.DescriptorProto(), maybeQualifiedName)
}
