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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// systemTableIDResolver is the implementation for catalog.SystemTableIDResolver.
type systemTableIDResolver struct {
	collectionFactory *CollectionFactory
	ie                sqlutil.InternalExecutor
	db                *kv.DB
}

var _ catalog.SystemTableIDResolver = (*systemTableIDResolver)(nil)

// MakeSystemTableIDResolver creates an object that implements catalog.SystemTableIDResolver.
func MakeSystemTableIDResolver(
	collectionFactory *CollectionFactory, ie sqlutil.InternalExecutor, db *kv.DB,
) catalog.SystemTableIDResolver {
	return &systemTableIDResolver{
		collectionFactory: collectionFactory,
		ie:                ie,
		db:                db,
	}
}

// LookupSystemTableID implements the catalog.SystemTableIDResolver method.
func (r *systemTableIDResolver) LookupSystemTableID(
	ctx context.Context, tableName string,
) (descpb.ID, error) {

	var id descpb.ID
	if err := r.collectionFactory.Txn(ctx, r.ie, r.db, func(
		ctx context.Context, txn *kv.Txn, descriptors *Collection,
	) (err error) {
		id, err = descriptors.kv.lookupName(
			ctx, txn, nil /* maybeDatabase */, keys.SystemDatabaseID, keys.PublicSchemaID, tableName,
		)
		return err
	}); err != nil {
		return 0, err
	}
	return id, nil
}
