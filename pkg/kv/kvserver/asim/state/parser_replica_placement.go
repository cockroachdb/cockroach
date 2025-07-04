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
		s := "s" + strconv.Itoa(r.StoreIDs[i]) + ":"
		if r.Types[i] != roachpb.VOTER_FULL {
			s += r.Types[i].String()
		}
		if r.StoreIDs[i] == r.LeaseholderID {
			s += "*"
		}
		if l := len(s); s[l-1] == ':' {
			s = s[:l-1] // remove trailing ':'
		}
		storeAndTypes[i] = s
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

// ParseReplicaPlacement parses replica placement rules in the format "{stores}:weight".
// Weights determine the fraction of ranges using each placement pattern. Examples:
// {s1:*,s2,s3:NON_VOTER}:1 {s4:*,s5,s6}:1 means half of ranges: s1(leaseholder),s2,s3(non-voter)
// and half of ranges: s4(leaseholder),s5,s6.
func ParseReplicaPlacement(input string) ReplicaPlacement {
	pattern := `\{([^}]+)\}:(\d+)`
	re := regexp.MustCompile(pattern)

	// Consider input "{s1:*,s2,s3:NON_VOTER}:1 {s4:*,s5,s6}:1".
	var result []Ratio
	matches := re.FindAllStringSubmatch(input, -1)

	// matches[0] will be []string{"{s1:*,s2,s3:NON_VOTER}:1", "s1:*,s2,s3:NON_VOTER", "1"}
	// matches[1] will be []string{"{s4:*,s5,s6}:1", "s4:*,s5,s6", "1"}
	for _, match := range matches {
		// For matches[0], stores will be []string{"s1:*","s2","s3:NON_VOTER"}.
		stores := strings.Split(match[1], ",")
		// For matches[0], weight will be 1.
		weight, _ := strconv.Atoi(match[2])

		storeSet := make([]int, 0)
		typeSet := make([]roachpb.ReplicaType, 0)
		var leaseholderStoreID int
		foundLeaseholder := false

		for _, store := range stores {
			store = strings.TrimSpace(store)
			parts := strings.Split(store, ":")
			if len(parts) == 0 {
				panic(fmt.Sprintf("invalid replica placement: %s", input))
			}
			// For matches[0] and stores[0], parts will be []string{"s1","*"}.
			if !strings.HasPrefix(parts[0], "s") {
				panic(fmt.Sprintf("invalid replica placement: %s", input))
			}
			// For matches[0] and stores[0], storeID will be 1.
			storeID, _ := strconv.Atoi(parts[0][1:])
			if storeID == 0 {
				panic(fmt.Sprintf("unable to parse store id: %s", parts[0][1:]))
			}
			storeSet = append(storeSet, storeID)

			// If the replica type or leaseholder is not specified, artificially
			// append VOTER_FULL to parts. For matches[0] and stores[1], parts
			// will be []string{"s2","VOTER_FULL"}.
			if len(parts) < 2 {
				parts = append(parts, "VOTER_FULL")
			}
			if len(parts) < 2 {
				panic(fmt.Sprintf("invalid replica placement: %s", input))
			}
			// For matches[0] and stores[0], typ will be "VOTER_FULL".
			typ := parts[1]
			// If replica is a leaseholder, indicate the leaseholder store ID. Note
			// that incoming voter may also be a leaseholder.
			if last := len(typ) - 1; typ[last] == '*' {
				leaseholderStoreID = storeID
				foundLeaseholder = true
				typ = typ[:last] // remove '*'
			}
			if typ == "" {
				typ = roachpb.VOTER_FULL.String() // default type
			}
			v, ok := roachpb.ReplicaType_value[typ]
			if !ok {
				panic(fmt.Sprintf("unknown replica type: %s", typ))
			}
			typeSet = append(typeSet, roachpb.ReplicaType(v))
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
