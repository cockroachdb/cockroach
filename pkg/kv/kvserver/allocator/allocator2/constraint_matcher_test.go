// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allocator2

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func parseStoreDescriptor(t *testing.T, d *datadriven.TestData) roachpb.StoreDescriptor {
	var desc roachpb.StoreDescriptor
	var storeID int
	d.ScanArgs(t, "store-id", &storeID)
	desc.StoreID = roachpb.StoreID(storeID)
	var nodeID int
	if d.HasArg("node-id") {
		d.ScanArgs(t, "node-id", &nodeID)
		desc.Node.NodeID = roachpb.NodeID(nodeID)
	}
	var attrs string
	d.ScanArgs(t, "attrs", &attrs)
	for _, v := range strings.Split(attrs, ",") {
		v = strings.TrimSpace(v)
		desc.Attrs.Attrs = append(desc.Attrs.Attrs, v)
	}
	var lts string
	d.ScanArgs(t, "locality-tiers", &lts)
	for _, v := range strings.Split(lts, ",") {
		v = strings.TrimSpace(v)
		kv := strings.Split(v, "=")
		require.Equal(t, 2, len(kv))
		desc.Node.Locality.Tiers = append(
			desc.Node.Locality.Tiers, roachpb.Tier{Key: kv[0], Value: kv[1]})
	}
	return desc
}

func TestConstraintMatcher(t *testing.T) {
	interner := newStringInterner()
	cm := newConstraintMatcher(interner)

	datadriven.RunTest(t, "testdata/constraint_matcher",
		func(t *testing.T, d *datadriven.TestData) string {
			printMatcher := func(b *strings.Builder) {
				type constraintAndPL struct {
					c  internedConstraint
					pl *matchedSet
				}
				var cpls []constraintAndPL
				for c, pl := range cm.constraints {
					cpls = append(cpls, constraintAndPL{c: c, pl: pl})
				}
				sort.Slice(cpls, func(i, j int) bool {
					return cpls[i].c.less(cpls[j].c)
				})
				for _, cpl := range cpls {
					c := cpl.c
					pl := cpl.pl
					rc := roachpb.Constraint{
						Type:  c.typ,
						Key:   interner.toString(c.key),
						Value: interner.toString(c.value),
					}
					sepStr := ""
					if len(pl.storeIDPostingList) > 0 {
						sepStr = " "
					}
					fmt.Fprintf(b, "%s:%s", rc.String(), sepStr)
					printPostingList(b, pl.storeIDPostingList)
					fmt.Fprintf(b, "\n")
				}
				err := cm.checkConsistency()
				require.NoError(t, err)
			}

			switch d.Cmd {
			case "store":
				desc := parseStoreDescriptor(t, d)
				cm.setStore(desc)
				var b strings.Builder
				printMatcher(&b)
				return b.String()

			case "remove-store":
				var storeID int
				d.ScanArgs(t, "store-id", &storeID)
				cm.removeStore(roachpb.StoreID(storeID))
				var b strings.Builder
				printMatcher(&b)
				return b.String()

			case "store-matches":
				var storeID int
				d.ScanArgs(t, "store-id", &storeID)
				cc := parseConstraints(t, strings.Fields(strings.Split(d.Input, "\n")[0]))
				matches := cm.storeMatches(roachpb.StoreID(storeID), interner.internConstraints(cc))
				return fmt.Sprintf("%t", matches)

			case "match-stores":
				var disj constraintsDisj
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					cc := parseConstraints(t, parts)
					disj = append(disj, interner.internConstraints(cc))
				}
				var pl storeIDPostingList
				if len(disj) == 1 {
					cm.constrainStoresForConjunction(disj[0], &pl)
				} else {
					cm.constrainStoresForExpr(disj, &pl)
				}
				var b strings.Builder
				printPostingList(&b, pl)
				return b.String()

			case "print":
				var b strings.Builder
				printMatcher(&b)
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
