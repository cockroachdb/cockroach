// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvaccessor

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

var (
	// NoopKVAccessor is a KVAccessor that simply no-ops (writing nothing,
	// returning nothing).
	NoopKVAccessor = dummyKVAccessor{error: nil}

	// DisabledKVAccessor is a KVAccessor that only returns "disabled" errors.
	DisabledKVAccessor = dummyKVAccessor{error: errors.New("span configs disabled")}
)

// dummyKVAccessor is a KVAccessor that simply returns the embedded
// error.
type dummyKVAccessor struct {
	error error
}

var _ spanconfig.KVAccessor = &dummyKVAccessor{}

// GetSpanConfigRecords is part of the KVAccessor interface.
func (k dummyKVAccessor) GetSpanConfigRecords(
	context.Context, []spanconfig.Target,
) ([]spanconfig.Record, error) {
	return nil, k.error
}

// UpdateSpanConfigRecords is part of the KVAccessor interface.
func (k dummyKVAccessor) UpdateSpanConfigRecords(
	context.Context, []spanconfig.Target, []spanconfig.Record, hlc.Timestamp, hlc.Timestamp,
) error {
	return k.error
}

// GetAllSystemSpanConfigsThatApply is part of the spanconfig.KVAccessor
// interface.
func (k dummyKVAccessor) GetAllSystemSpanConfigsThatApply(
	context.Context, roachpb.TenantID,
) ([]roachpb.SpanConfig, error) {
	return nil, k.error
}

func (k dummyKVAccessor) WithTxn(context.Context, *kv.Txn) spanconfig.KVAccessor {
	return k
}

// WithISQLTxn is part of the KVAccessor interface.
func (k dummyKVAccessor) WithISQLTxn(ctx context.Context, txn isql.Txn) spanconfig.KVAccessor {
	return k
}
