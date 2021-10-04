// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ShadowReader wraps around two spanconfig.StoreReaders and logs warnings (if
// expensive logging is enabled) when there are divergent results from the two.
type ShadowReader struct {
	new, old spanconfig.StoreReader
}

// NewShadowReader instantiates a new shadow reader.
func NewShadowReader(new, old spanconfig.StoreReader) *ShadowReader {
	return &ShadowReader{
		new: new,
		old: old,
	}
}

var _ = NewShadowReader // defeat the unused linter.

var _ spanconfig.StoreReader = &ShadowReader{}

// NeedsSplit is part of the spanconfig.StoreReader interface.
func (s *ShadowReader) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	newResult := s.new.NeedsSplit(ctx, start, end)
	if log.ExpensiveLogEnabled(ctx, 1) {
		oldResult := s.old.NeedsSplit(ctx, start, end)
		if newResult != oldResult {
			log.Warningf(ctx, "needs split: mismatched responses between old result (%t) and new (%t) for start=%s end=%s",
				oldResult, newResult, start.String(), end.String())
		}
	}

	return newResult
}

// ComputeSplitKey is part of the spanconfig.StoreReader interface.
func (s *ShadowReader) ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey {
	newResult := s.new.ComputeSplitKey(ctx, start, end)
	if log.ExpensiveLogEnabled(ctx, 1) {
		oldResult := s.old.ComputeSplitKey(ctx, start, end)
		if !newResult.Equal(oldResult) {
			str := func(k roachpb.RKey) string {
				if len(k) == 0 {
					return ""
				}
				return k.String()
			}

			log.Warningf(ctx, "compute split key: mismatched responses between old result (%s) and new (%s) for start=%s end=%s",
				str(oldResult), str(newResult), str(start), str(end))
		}
	}
	return newResult
}

// GetSpanConfigForKey is part of the spanconfig.StoreReader interface.
func (s *ShadowReader) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	newResult, errNew := s.new.GetSpanConfigForKey(ctx, key)
	if log.ExpensiveLogEnabled(ctx, 1) {
		oldResult, errOld := s.old.GetSpanConfigForKey(ctx, key)
		if !newResult.Equal(oldResult) {
			log.Warningf(ctx, "get span config for key: mismatched responses between old result (%s) and new(%s) for key=%s",
				oldResult.String(), newResult.String(), key.String())
		}
		if !errors.Is(errNew, errOld) {
			log.Warningf(ctx, "get span config for key: mismatched errors between old result (%s) and new (%s) for key=%s",
				errOld, errNew, key.String())
		}
	}
	return newResult, errNew
}
