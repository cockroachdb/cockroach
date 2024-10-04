// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// rangeDescIterator is the concrete (private) implementation of the
// rangedesc.Iterator interface used by the connector.
type rangeDescIterator struct {
	rangeDescs []roachpb.RangeDescriptor
	curIdx     int
}

// Valid implements the rangedesc.Iterator interface.
func (i *rangeDescIterator) Valid() bool {
	return i.curIdx < len(i.rangeDescs)
}

// Next implements the rangedesc.Iterator interface.
func (i *rangeDescIterator) Next() {
	i.curIdx++
}

// CurRangeDescriptor implements the rangedesc.Iterator interface.
func (i *rangeDescIterator) CurRangeDescriptor() roachpb.RangeDescriptor {
	return i.rangeDescs[i.curIdx]
}
