// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Storage is used to interact with the data storage layer for the
// singleversion subsystem.
type Storage interface {

	// Scan will scan the rows in the specified prefixes. Note that row entries
	// with an empty session ID will scan for all rows under the prefix of the
	// specified action and descriptor ID.
	Scan(
		ctx context.Context, txn *kv.Txn, prefixes []Row, resultFunc func(Row),
	) (err error)

	// Delete will blindly delete the specified rows.
	Delete(ctx context.Context, txn *kv.Txn, rows ...Row) error

	// Put will blindly write the specified rows.
	Put(ctx context.Context, txn *kv.Txn, rows ...Row) error

	// RangeFeed runs a rangefeed with the specified start over the specified
	// action prefix. Events are passed to the callback, which is expected to
	// not block.
	RangeFeed(
		ctx context.Context, rf *rangefeed.Factory, action Action, ts hlc.Timestamp, f func(re Event),
	) error
}

// NewStorage constructs a new Storage.
func NewStorage(codec keys.SQLCodec, svLeaseTableID descpb.ID) Storage {
	return &storage{
		th: tableCodec{
			codec:   codec,
			tableID: svLeaseTableID,
		},
	}
}

// storage represents the way
type storage struct {
	th tableCodec
}

func (s *storage) Scan(
	ctx context.Context, txn *kv.Txn, prefixes []Row, resultFunc func(Row),
) (err error) {
	return scan(ctx, txn, s.th, prefixes, resultFunc)
}

func (s *storage) Delete(ctx context.Context, txn *kv.Txn, rows ...Row) error {
	return deleteRows(ctx, txn, s.th, rows)
}

func (s *storage) RangeFeed(
	ctx context.Context, rf *rangefeed.Factory, action Action, ts hlc.Timestamp, f func(re Event),
) error {
	return runRangefeed(ctx, rf, s.th, action, ts, f)
}

func (s *storage) Put(ctx context.Context, txn *kv.Txn, rows ...Row) error {
	return putRows(ctx, txn, s.th, rows)
}
