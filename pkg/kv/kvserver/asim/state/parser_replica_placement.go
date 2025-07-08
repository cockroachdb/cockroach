// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ReplicaPlacement is a list of replica placement ratios. Each ratio represents
// a % of ranges that should be placed using the given store IDs and types. For
// example, a ratio of {s1:*,s2,s3:NON_VOTER}:1 present in a ReplicaPlacement of
// total weight 3 means 1/3 of ranges should be placed with replicas on s1, s2,
// and s3, s1 as the leaseholder, and the replica on s3 as a non-voter.
//
// See TestReplicaPlacement for more examples.
type ReplicaPlacement []Ratio

// findReplicaPlacementForEveryStoreSet finds the replica placement for every
// store set. We are overloading the Weight field here to calculate the actual
// number of ranges to place with the given ratio.
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
	// Distribute the remaining ranges evenly across all ratios.
	for i := 0; i < totalRangesToAllocate; i++ {
		pr[i%len(pr)].Weight += 1
	}
}

// Ratio struct to represent weight and store IDs
type Ratio struct {
	// Weight is the relative weight of this ratio. It is used to calculate the
	// number of ranges to place with the given ratio. findReplicaPlacementForEveryStoreSet
	// later re-assigns the Weight field to the actual number of ranges to place.
	Weight int
	// StoreIDs is the list of store IDs to place replicas on.
	StoreIDs []int
	// Types is the list of replica types. 1:1 mapping to StoreIDs. Default is
	// VOTER_FULL.
	Types []roachpb.ReplicaType
	// LeaseholderID is the store ID of the leaseholder for this ratio. Default is
	// the first store in StoreIDs.
	LeaseholderID int
}

func (r Ratio) String() string {
	storeAndTypes := make([]string, len(r.StoreIDs))
	for i := range r.StoreIDs {
		storeAndTypes[i] = "s" + strconv.Itoa(r.StoreIDs[i])
		if r.Types[i] == roachpb.NON_VOTER {
			storeAndTypes[i] += ":NON_VOTER"
		}
		if r.StoreIDs[i] == r.LeaseholderID {
			storeAndTypes[i] += ":leaseholder"
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

// ParseStoreWeights parses replica placement rules in the format "{stores}:weight".
// Weights determine the fraction of ranges using each placement pattern. Examples:
// {s1:*,s2,s3:NON_VOTER}:1 {s4:*,s5,s6}:1 means half of ranges: s1(leaseholder),s2,s3(non-voter)
// and half of ranges: s4(leaseholder),s5,s6.
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
		var leaseholderStoreID int
		foundLeaseholder := false

		for _, store := range stores {
			store = strings.TrimSpace(store)
			parts := strings.Split(store, ":")
			if strings.HasPrefix(parts[0], "s") {
				storeID, _ := strconv.Atoi(parts[0][1:])
				storeSet = append(storeSet, storeID)

				replicaType := roachpb.VOTER_FULL
				if len(parts) > 1 {
					switch parts[1] {
					case "NON_VOTER":
						replicaType = roachpb.NON_VOTER
					case "*":
						leaseholderStoreID = storeID
						foundLeaseholder = true
					default:
						panic(fmt.Sprintf("unknown replica type: %s", parts[1]))
					}
				}
				typeSet = append(typeSet, replicaType)
			}
		}

		if !foundLeaseholder {
			// Use the first store as the leaseholder.
			leaseholderStoreID = storeSet[0]
		}

		ratio := Ratio{
			Weight:        weight,
			StoreIDs:      storeSet,
			Types:         typeSet,
			LeaseholderID: leaseholderStoreID,
		}
		result = append(result, ratio)
	}

	return result
}
