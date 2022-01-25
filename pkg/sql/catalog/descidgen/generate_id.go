// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descidgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
//
// TODO(postamar): define an interface for ID generation,
// both the db and codec dependencies are singletons anyway.
func GenerateUniqueDescID(ctx context.Context, db *kv.DB, codec keys.SQLCodec) (descpb.ID, error) {
	// Increment unique descriptor counter.
	newVal, err := kv.IncrementValRetryable(ctx, db, codec.DescIDSequenceKey(), 1)
	if err != nil {
		return descpb.InvalidID, err
	}
	return descpb.ID(newVal - 1), nil
}

// PeekNextUniqueDescID returns the next as-of-yet unassigned unique descriptor
// ID in the sequence. Note that this value is _not_ guaranteed to be the same
// as that returned by a subsequent call to GenerateUniqueDescID. It will,
// however, be a lower bound on it.
func PeekNextUniqueDescID(ctx context.Context, db *kv.DB, codec keys.SQLCodec) (descpb.ID, error) {
	v, err := db.Get(ctx, codec.DescIDSequenceKey())
	if err != nil {
		return descpb.InvalidID, err
	}
	return descpb.ID(v.ValueInt()), nil
}
