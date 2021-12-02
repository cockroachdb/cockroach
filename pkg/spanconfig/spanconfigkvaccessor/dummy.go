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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/errors"
)

// dummyKVAccessor is a KVAccessor{,WithTxn} that simply returns the embedded
// error.
type dummyKVAccessor struct {
	error error
}

var _ spanconfig.KVAccessor = &dummyKVAccessor{}
var _ spanconfig.KVAccessorWithTxn = &dummyKVAccessor{}

// MakeNoopKVAccessorWithTxn returns a KVAccessorWithTxn that simply no-ops
// (writing nothing, returning nothing).
func MakeNoopKVAccessorWithTxn() spanconfig.KVAccessorWithTxn {
	return dummyKVAccessor{error: nil}
}

// MakeIllegalKVAccessorWithTxn returns a KVAccessorWithTxn that only returns
// "illegal use" errors.
func MakeIllegalKVAccessorWithTxn() spanconfig.KVAccessorWithTxn {
	return dummyKVAccessor{error: errors.New("illegal use of kvaccessor")}
}

// MakeDisabledKVAccessor returns a KVAccessor that only returns "disabled"
// errors.
func MakeDisabledKVAccessor() spanconfig.KVAccessor {
	return dummyKVAccessor{error: errors.New("span configs disabled")}
}

// GetSpanConfigEntriesFor is part of the KVAccessor interface.
func (k dummyKVAccessor) GetSpanConfigEntriesFor(
	context.Context, []roachpb.Span,
) ([]roachpb.SpanConfigEntry, error) {
	return nil, k.error
}

// UpdateSpanConfigEntries is part of the KVAccessor interface.
func (k dummyKVAccessor) UpdateSpanConfigEntries(
	context.Context, []roachpb.Span, []roachpb.SpanConfigEntry,
) error {
	return k.error
}

// GetSpanConfigEntriesForWithTxn is part of the KVAccessorWithTxn interface.
func (k dummyKVAccessor) GetSpanConfigEntriesForWithTxn(
	context.Context, []roachpb.Span, *kv.Txn,
) ([]roachpb.SpanConfigEntry, error) {
	return nil, k.error
}

// UpdateSpanConfigEntriesWithTxn is part of the KVAccessorWithTxn interface.
func (k dummyKVAccessor) UpdateSpanConfigEntriesWithTxn(
	context.Context, []roachpb.Span, []roachpb.SpanConfigEntry, *kv.Txn,
) error {
	return k.error
}
