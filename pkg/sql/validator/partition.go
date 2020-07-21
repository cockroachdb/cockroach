// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validator

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

type partitionInterval struct {
	name  string
	start roachpb.Key
	end   roachpb.Key
}

var _ interval.Interface = partitionInterval{}

// ID is part of `interval.Interface` but unused in validatePartitioningDescriptor.
func (ps partitionInterval) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ps partitionInterval) Range() interval.Range {
	return interval.Range{Start: []byte(ps.start), End: []byte(ps.end)}
}
