// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
