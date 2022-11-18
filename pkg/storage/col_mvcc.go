// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// NextKVer can fetch a new KV from somewhere. If MVCCDecodingStrategy is set
// to required, the returned KV will include a timestamp.
type NextKVer interface {
	// NextKV returns the next kv from this NextKVer. Returns false if there are
	// no more kvs to fetch, the kv that was fetched, and any errors that may
	// have occurred.
	//
	// finalReferenceToBatch is set to true if the returned KV's byte slices are
	// the last reference into a larger backing byte slice. This parameter
	// allows calling code to control its memory usage: if finalReferenceToBatch
	// is true, it means that the next call to NextKV might potentially allocate
	// a big chunk of new memory, so the returned KeyValue should be copied into
	// a small slice that the caller owns to avoid retaining two large backing
	// byte slices at once unexpectedly.
	NextKV(context.Context, MVCCDecodingStrategy) (
		ok bool, kv roachpb.KeyValue, finalReferenceToBatch bool, err error,
	)
}
