// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// mergeDistSQLRemoteFlows takes in two slices of DistSQL remote flows (that
// satisfy the contract of serverpb.ListDistSQLFlowsResponse) and merges them
// together while adhering to the same contract.
//
// It is assumed that if serverpb.DistSQLRemoteFlows for a particular FlowID
// appear in both arguments - let's call them flowsA and flowsB for a and b,
// respectively - then there are no duplicate NodeIDs among flowsA and flowsB.
func mergeDistSQLRemoteFlows(a, b []serverpb.DistSQLRemoteFlows) []serverpb.DistSQLRemoteFlows {
	maxLength := len(a)
	if len(b) > len(a) {
		maxLength = len(b)
	}
	result := make([]serverpb.DistSQLRemoteFlows, 0, maxLength)
	aIter, bIter := 0, 0
	for aIter < len(a) && bIter < len(b) {
		cmp := bytes.Compare(a[aIter].FlowID.GetBytes(), b[bIter].FlowID.GetBytes())
		if cmp < 0 {
			result = append(result, a[aIter])
			aIter++
		} else if cmp > 0 {
			result = append(result, b[bIter])
			bIter++
		} else {
			r := a[aIter]
			// No need to perform any kind of de-duplication because a
			// particular flow will be reported at most once by each node in the
			// cluster.
			r.Infos = append(r.Infos, b[bIter].Infos...)
			sort.Slice(r.Infos, func(i, j int) bool {
				return r.Infos[i].NodeID < r.Infos[j].NodeID
			})
			result = append(result, r)
			aIter++
			bIter++
		}
	}
	if aIter < len(a) {
		result = append(result, a[aIter:]...)
	}
	if bIter < len(b) {
		result = append(result, b[bIter:]...)
	}
	return result
}
