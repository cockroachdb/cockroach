// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

// Filter informs the producer of logical operations of the information that a
// rangefeed Processor is interested in, given its current set of registrations.
// It can be used to avoid performing extra work to provide the Processor with
// information which will be ignored.
type Filter struct {
	needVals interval.RangeGroup
	// TODO(nvanbenschoten): When addressing #28666, we can extend this with
	// a second `needPreVals interval.RangeGroup` to avoid fetching existing
	// values for spans that don't need them.
}

func newFilterFromRegistry(reg *registry) *Filter {
	f := &Filter{
		needVals: interval.NewRangeList(),
	}
	reg.tree.Do(func(i interval.Interface) (done bool) {
		r := i.(*registration)
		f.needVals.Add(r.Range())
		return false
	})
	return f
}

// NeedVal returns whether the Processor requires MVCCWriteValueOp and
// MVCCCommitIntentOp operations over the specified key span to contain
// populated Value fields.
func (r *Filter) NeedVal(s roachpb.Span) bool {
	return r.needVals.Overlaps(s.AsRange())
}
