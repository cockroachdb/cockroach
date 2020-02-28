// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
)

// Index maintains a precomputed data structure for accelerating a small number
// of spatial operations.
//
// Specifically:
// - Contains (ST_Contains)
// - Bounding box contains (&&)
// - [Future work]: Intersects, Within, Nearest Neighbors (ST_Distance)
//
// An index is constructed with a protobuf configuration. The same config must
// be used for all reads and writes to that index. Changing an index
// configuration is a schema change and may require altering the stored data.
//
// Index does not actually persist the data structure, it is expected to be
// stored externally. This store must support key-value semantics, range queries
// over keys, and duplicated keys. Unit tests use a btree and CockroachDB itself
// uses the replicated KV layer.
//
// Spatial data is added to the index by calling InvertedIndexKeys, this returns
// a set of uint64 keys the given object must be stored under.
//
// A query on that indexed data is constructed using one of the other methods on
// Index, such as Contains. This query is executed in two parts. First a
// retrieval pass collects all stored entries in a set of ranges of keys. This
// may contain duplicates and false positives, but it's guaranteed to contain
// all true positives. Second, an exact filtering step is applied to remove the
// false positives.
//
// Note that the optimizer is free to reorder this filtering to whenever it
// chooses, which can be used to first execute a more restrictive predicate from
// elsewhere in the SQL query.
type Index interface {
	// InvertedIndexKeys returns the keys to store this object under when adding
	// it to the index.
	InvertedIndexKeys(context.Context, geo.Region) ([]Key, error)

	// Contains is an index-accelerated execution of ST_Contains.
	Contains(context.Context, geo.Region) (KeySpanQuery, error)

	// AtOperator is an index-accelerated execution of the @ operator.
	AtOperator(context.Context, geo.Region) (KeySpanQuery, error)
}

// New returns an Index implementation with the given configuration. All reads
// and writes on this index must use the same config.
func New(cfg Config) (Index, error) {
	switch c := cfg.GetValue().(type) {
	case *S2Config:
		return NewS2Index(*c), nil
	default:
		return nil, errors.AssertionFailedf(`unknown index config type %T: %v`, c, c)
	}
}

// Key is one entry under which a geospatial object is stored on behalf of an
// Index.
type Key uint64

// KeySpan represents a range of Keys.
type KeySpan struct {
	// NB In contract to most of our Spans, End is inclusive.
	Start, End Key
}

// KeySpanQuery is an execution plan for an index-accelerated spatial query
// plan. See Index for details.
type KeySpanQuery struct {
	// Union is the set of indexed keys to retrieve. Duplicates primary keys will
	// not be retrieved by any individual span, but they may be present if more
	// than one span is retrieved. Additionally, false positives may be retrieved,
	// these are handled by Filter.
	Union []KeySpan
	// Filter, if non-nil is a function that returns true for each value that
	// should be kept. If filter is nil, no post-retrieval filtering should be
	// done.
	//
	// TODO(dan): This is likely not the best way to communicate this to the
	// optimizer, what does it want here?
	Filter func(geo.Region, geo.Region) bool
	// EstimatedSelectivity is a guess at the ratio for how many of the retrived,
	// uniqued values will be kept by Filter. It is in the range [0,1].
	EstimatedSelectivity float64
}
