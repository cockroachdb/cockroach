// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvaccessor

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/errors"
)

// DisabledAccessor provides a implementation of the KVAccessor interface that
// simply errors out with a disabled error.
type DisabledAccessor struct{}

// GetSpanConfigEntriesFor is part of the KVAccessor interface.
func (n DisabledAccessor) GetSpanConfigEntriesFor(
	context.Context, []roachpb.Span,
) ([]roachpb.SpanConfigEntry, error) {
	return nil, errors.New("span configs disabled")
}

// UpdateSpanConfigEntries is part of the KVAccessor interface.
func (n DisabledAccessor) UpdateSpanConfigEntries(
	context.Context, []roachpb.Span, []roachpb.SpanConfigEntry,
) error {
	return errors.New("span configs disabled")
}

var _ spanconfig.KVAccessor = &DisabledAccessor{}
