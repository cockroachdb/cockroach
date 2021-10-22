// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type storeIDSet map[roachpb.StoreID]struct{}

// storeListFromSet unwraps map to a sorted list of StoreIDs.
func storeListFromSet(set storeIDSet) []roachpb.StoreID {
	storeIDs := make([]roachpb.StoreID, 0, len(set))
	for k := range set {
		storeIDs = append(storeIDs, k)
	}
	sort.Slice(storeIDs, func(i, j int) bool {
		return storeIDs[i] < storeIDs[j]
	})
	return storeIDs
}

// Make a string of stores 'set' in ascending order.
func joinStoreIDs(storeIDs storeIDSet) string {
	storeNames := make([]string, 0, len(storeIDs))
	for _, id := range storeListFromSet(storeIDs) {
		storeNames = append(storeNames, fmt.Sprintf("s%d", id))
	}
	return strings.Join(storeNames, ", ")
}
