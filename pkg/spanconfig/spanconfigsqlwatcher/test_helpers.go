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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// TestingGenerateSpanConfigurationsForTable is a wrapper around
// generateSpanConfigurationsForTable for testing purposes.
func (s *SQLWatcher) TestingGenerateSpanConfigurationsForTable(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) ([]roachpb.SpanConfigEntry, error) {
	return s.generateSpanConfigurationsForTable(ctx, txn, id)
}
