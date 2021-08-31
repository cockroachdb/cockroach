// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
)

// TestingHandleIDUpdate is a wrapper around handleIDUpdate for testing
// purposes.
func (s *SQLWatcher) TestingHandleIDUpdate(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) ([]spanconfig.Update, error) {
	return s.handleIDUpdate(ctx, id, txn, descsCol)
}
