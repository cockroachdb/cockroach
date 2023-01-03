// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
)

var _ scexec.IndexSpanSplitter = (*indexSpanSplitter)(nil)

type indexSpanSplitter struct{}

// MaybeSplitIndexSpans implements the scexec.IndexSpanSplitter interface.
func (s *indexSpanSplitter) MaybeSplitIndexSpans(
	_ context.Context, _ catalog.TableDescriptor, _ catalog.Index,
) error {
	return nil
}

func (s *indexSpanSplitter) MaybeSplitIndexSpansForPartitioning(
	_ context.Context, _ catalog.TableDescriptor, _ catalog.Index,
) error {
	return nil
}
