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

// ShadowReader wraps around two spanconfig.StoreReaders and logs warnings when
// there are divergent results from the two.
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

var _ spanconfig.StoreReader = &ShadowReader{}

// NeedsSplit is part of the spanconfig.StoreReader interface.
func (s *ShadowReader) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	new := s.new.NeedsSplit(ctx, start, end)
	old := s.old.NeedsSplit(ctx, start, end)
	if new != old && log.ExpensiveLogEnabled(ctx, 1) {
		log.Warningf(ctx, "needs split: mismatched responses between old(%t) and new(%t) for start=%s end=%s",
			old, new, start.String(), end.String())
	}
	return new
}

// ComputeSplitKey is part of the spanconfig.StoreReader interface.
func (s *ShadowReader) ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey {
	new := s.new.ComputeSplitKey(ctx, start, end)
	old := s.old.ComputeSplitKey(ctx, start, end)
	if !new.Equal(old) && log.ExpensiveLogEnabled(ctx, 1) {
		str := func(k roachpb.RKey) string {
			if len(k) == 0 {
				return ""
			}
			return k.String()
		}

		log.Warningf(ctx, "compute split key: mismatched responses between old(%s) and new(%s) for start=%s end=%s",
			str(old), str(new), str(start), str(end))
	}
	return new
}

// GetSpanConfigForKey is part of the spanconfig.StoreReader interface.
func (s *ShadowReader) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	new, errNew := s.new.GetSpanConfigForKey(ctx, key)
	old, errOld := s.old.GetSpanConfigForKey(ctx, key)
	if log.ExpensiveLogEnabled(ctx, 1) {
		if !new.Equal(old) {
			log.Warningf(ctx, "get span config for key: mismatched responses between old(%s) and new(%s) for key=%s", old.String(), new.String(), key.String())
		}
		if !errors.Is(errNew, errOld) {
			log.Warningf(ctx, "get span config for key: mismatched errors between old(%s) and new(%s) for key=%s", errOld, errNew, key.String())
		}
	}
	return new, errNew
}
