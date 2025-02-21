// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDiversityScoringMemo(t *testing.T) {
	interner := newStringInterner()
	ltInterner := newLocalityTierInterner(interner)
	storeLocalities := map[int]localityTiers{}
	dsm := newDiversityScoringMemo()
	var lastReplicaLocalities *existingReplicaLocalities

	datadriven.RunTest(t, "testdata/diversity_scoring_memo",
		func(t *testing.T, d *datadriven.TestData) string {
			scanStores := func() []localityTiers {
				var storesStr string
				d.ScanArgs(t, "store-ids", &storesStr)
				storesStrSlice := strings.Split(storesStr, ",")
				var storeIDs []int
				for _, s := range storesStrSlice {
					storeID, err := strconv.Atoi(s)
					require.NoError(t, err)
					storeIDs = append(storeIDs, storeID)
				}
				var res []localityTiers
				for _, storeID := range storeIDs {
					l, ok := storeLocalities[storeID]
					require.True(t, ok)
					res = append(res, l)
				}
				return res
			}
			printReplicaLocalities := func(b *strings.Builder, rls *existingReplicaLocalities) {
				b.WriteString("replicas:\n")
				for _, rl := range rls.replicasLocalityTiers.replicas {
					fmt.Fprintf(b, "  %s\n", ltInterner.unintern(rl).String())
				}
				b.WriteString("score-sums:\n")
				var keys []string
				for k := range rls.scoreSums {
					keys = append(keys, k)
				}
				slices.Sort(keys)
				for _, k := range keys {
					fmt.Fprintf(b, "  locality-str %s: %f\n", k, rls.scoreSums[k])
				}
			}
			getStoreLocality := func(key string) localityTiers {
				var storeID int
				d.ScanArgs(t, key, &storeID)
				l, ok := storeLocalities[storeID]
				require.True(t, ok)
				return l
			}
			switch d.Cmd {
			case "store":
				var storeID int
				d.ScanArgs(t, "store-id", &storeID)
				var lts string
				d.ScanArgs(t, "locality-tiers", &lts)
				locality := parseLocalityTiers(t, d, lts)
				lt := ltInterner.intern(locality)
				storeLocalities[storeID] = lt
				return fmt.Sprintf("locality: %s str: %s", ltInterner.unintern(lt).String(), lt.str)

			case "existing-replica-localities":
				storeTiers := scanStores()
				lastReplicaLocalities = dsm.getExistingReplicaLocalities(storeTiers)
				var b strings.Builder
				printReplicaLocalities(&b, lastReplicaLocalities)
				fmt.Fprintf(&b, "num-existing-replica-localities: %d", dsm.replicasMap.lenForTesting())
				return b.String()

			case "score-new-replica":
				l := getStoreLocality("store-id")
				score := lastReplicaLocalities.getScoreChangeForNewReplica(l)
				var b strings.Builder
				fmt.Fprintf(&b, "score: %f\n", score)
				printReplicaLocalities(&b, lastReplicaLocalities)
				return b.String()

			case "score-remove-replica":
				l := getStoreLocality("store-id")
				score := lastReplicaLocalities.getScoreChangeForReplicaRemoval(l)
				var b strings.Builder
				fmt.Fprintf(&b, "score: %f\n", score)
				printReplicaLocalities(&b, lastReplicaLocalities)
				return b.String()

			case "score-rebalance":
				removeLocality := getStoreLocality("remove-store-id")
				addLocality := getStoreLocality("add-store-id")
				score := lastReplicaLocalities.getScoreChangeForRebalance(removeLocality, addLocality)
				var b strings.Builder
				fmt.Fprintf(&b, "score: %f\n", score)
				printReplicaLocalities(&b, lastReplicaLocalities)
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
