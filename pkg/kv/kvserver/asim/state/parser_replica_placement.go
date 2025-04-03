// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type ReplicaPlacement []Ratio

func (pr ReplicaPlacement) findReplicaPlacementForEveryStoreSet(numRanges int) {
	totalWeight := 0
	for i := 0; i < len(pr); i++ {
		totalWeight += pr[i].Weight
	}
	totalRangesToAllocate := numRanges
	for i := 0; i < len(pr); i++ {
		pr[i].Weight = int(float64(pr[i].Weight) * float64(numRanges) / float64(totalWeight))
		totalRangesToAllocate -= pr[i].Weight
	}
	for i := 0; i < totalRangesToAllocate; i++ {
		pr[i%len(pr)].Weight += 1
	}
}

// Ratio struct to represent weight and store IDs
type Ratio struct {
	Weight   int
	StoreIDs []int
	Types    []roachpb.ReplicaType
}

func (r Ratio) String() string {
	storeAndTypes := make([]string, len(r.StoreIDs))
	for i := range r.StoreIDs {
		storeAndTypes[i] = "s" + strconv.Itoa(r.StoreIDs[i])
		if r.Types[i] == roachpb.NON_VOTER {
			storeAndTypes[i] += ":NON_VOTER"
		}
	}
	return "{" + strings.Join(storeAndTypes, ",") + "}:" + strconv.Itoa(r.Weight)
}

func (pr ReplicaPlacement) String() string {
	var result []string
	for _, r := range pr {
		result = append(result, r.String())
	}
	return strings.Join(result, "\n")
}

func ParseStoreWeights(input string) ReplicaPlacement {
	pattern := `\{([^}]+)\}:(\d+)`
	re := regexp.MustCompile(pattern)

	var result []Ratio
	matches := re.FindAllStringSubmatch(input, -1)

	for _, match := range matches {
		stores := strings.Split(match[1], ",")
		weight, _ := strconv.Atoi(match[2])

		storeSet := make([]int, 0)
		typeSet := make([]roachpb.ReplicaType, 0)
		for _, store := range stores {
			store = strings.TrimSpace(store)
			parts := strings.Split(store, ":")
			if strings.HasPrefix(parts[0], "s") {
				storeID, _ := strconv.Atoi(parts[0][1:])
				storeSet = append(storeSet, storeID)

				replicaType := roachpb.VOTER_FULL
				if len(parts) > 1 && parts[1] == "NON_VOTER" {
					replicaType = roachpb.NON_VOTER
				}
				typeSet = append(typeSet, replicaType)
			}
		}

		result = append(result, Ratio{Weight: weight, StoreIDs: storeSet, Types: typeSet})
	}

	return result
}
