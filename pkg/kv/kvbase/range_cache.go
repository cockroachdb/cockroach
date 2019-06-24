// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package kvbase exports kv level interfaces to avoid dependency cycles.
package kvbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RangeDescriptorCache is a simplified interface to the kv.RangeDescriptorCache
// for use at lower levels of the stack like storage.
type RangeDescriptorCache interface {

	// LookupRangeDescritor looks up a range descriptor based on a key.
	LookupRangeDescriptor(
		ctx context.Context, key roachpb.RKey,
	) (*roachpb.RangeDescriptor, error)
}
