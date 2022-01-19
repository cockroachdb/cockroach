// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descmetadata

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// MakeConstraintOidBuilderFn creates a ConstraintOidBuilder.
type MakeConstraintOidBuilderFn func() ConstraintOidBuilder

// MetadataUpdaterFactory used to construct a commenter.DescriptorMetadataUpdater, which
// can be used to update comments on schema objects.
type MetadataUpdaterFactory struct {
	ieFactory                sqlutil.SessionBoundInternalExecutorFactory
	makeConstraintOidBuilder MakeConstraintOidBuilderFn
	collectionFactory        *descs.CollectionFactory
}

// NewMetadataUpdaterFactory creates a new comment updater factory.
func NewMetadataUpdaterFactory(
	ieFactory sqlutil.SessionBoundInternalExecutorFactory,
	makeConstraintOidBuilder MakeConstraintOidBuilderFn,
	collectionFactory *descs.CollectionFactory,
) scexec.DescriptorMetadataUpdaterFactory {
	return MetadataUpdaterFactory{
		ieFactory:                ieFactory,
		makeConstraintOidBuilder: makeConstraintOidBuilder,
		collectionFactory:        collectionFactory,
	}
}

// NewMetadataUpdater creates a new comment updater, which can be used to
// create / destroy metadata (i.e. comments) associated with different
// schema objects.
func (cf MetadataUpdaterFactory) NewMetadataUpdater(
	ctx context.Context, txn *kv.Txn, sessionData *sessiondata.SessionData,
) scexec.DescriptorMetadataUpdater {
	return metadataUpdater{
		txn:               txn,
		ie:                cf.ieFactory(ctx, sessionData),
		oidBuilder:        cf.makeConstraintOidBuilder(),
		collectionFactory: cf.collectionFactory,
	}
}
