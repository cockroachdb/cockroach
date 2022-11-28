// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// rangeDescIterator is the concrete (private) implementation of the
// rangedesc.Iterator interface used by the Connector.
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
