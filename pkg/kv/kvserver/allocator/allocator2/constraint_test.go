// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TODO: tests for
// - localityTierInterner
// - localityTiers.diversityScore
// - rangeAnalyzedConstraints initialization: pool and release; stateForInit, finishInit
// - rangeAnalyzedConstraints read-only functions.

func TestNormalizedSpanConfig(t *testing.T) {
	interner := newStringInterner()
	datadriven.RunTest(t, "testdata/normalize_config",
		func(t *testing.T, d *datadriven.TestData) string {
			parseConstraints := func(fields []string) []roachpb.Constraint {
				var cc []roachpb.Constraint
				for _, field := range fields {
					var typ roachpb.Constraint_Type
					switch field[0] {
					case '+':
						typ = roachpb.Constraint_REQUIRED
					case '-':
						typ = roachpb.Constraint_PROHIBITED
					default:
						t.Fatalf(fmt.Sprintf("unexpected start of field %s", field))
					}
					kv := strings.Split(field[1:], "=")
					if len(kv) != 2 {
						t.Fatalf("unexpected field %s", field)
					}
					cc = append(cc, roachpb.Constraint{
						Type:  typ,
						Key:   kv[0],
						Value: kv[1],
					})
				}
				return cc
			}
			parseConstraintsConj := func(fields []string) roachpb.ConstraintsConjunction {
				var cc roachpb.ConstraintsConjunction
				if strings.HasPrefix(fields[0], "num-replicas=") {
					val := strings.TrimPrefix(fields[0], "num-replicas=")
					replicas, err := strconv.Atoi(val)
					require.NoError(t, err)
					cc.NumReplicas = int32(replicas)
					fields = fields[1:]
				}
				cc.Constraints = parseConstraints(fields)
				return cc
			}
			printSpanConf := func(b *strings.Builder, conf roachpb.SpanConfig) {
				fmt.Fprintf(b, " num-replicas=%d num-voters=%d\n", conf.NumReplicas, conf.NumVoters)
				if len(conf.Constraints) > 0 {
					fmt.Fprintf(b, " constraints:\n")
					for _, cc := range conf.Constraints {
						fmt.Fprintf(b, "   %s\n", cc.String())
					}
				}
				if len(conf.VoterConstraints) > 0 {
					fmt.Fprintf(b, " voter-constraints:\n")
					for _, cc := range conf.VoterConstraints {
						fmt.Fprintf(b, "   %s\n", cc.String())
					}
				}
				if len(conf.LeasePreferences) > 0 {
					fmt.Fprintf(b, " lease-preferences:\n")
					for _, lp := range conf.LeasePreferences {
						fmt.Fprintf(b, "   ")
						for i, cons := range lp.Constraints {
							if i > 0 {
								b.WriteRune(',')
							}
							b.WriteString(cons.String())
						}
						fmt.Fprintf(b, "\n")
					}
				}
			}
			switch d.Cmd {
			case "normalize":
				var numReplicas, numVoters int
				var conf roachpb.SpanConfig
				d.ScanArgs(t, "num-replicas", &numReplicas)
				conf.NumReplicas = int32(numReplicas)
				d.ScanArgs(t, "num-voters", &numVoters)
				conf.NumVoters = int32(numVoters)
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					switch parts[0] {
					case "constraint":
						cc := parseConstraintsConj(parts[1:])
						conf.Constraints = append(conf.Constraints, cc)
					case "voter-constraint":
						cc := parseConstraintsConj(parts[1:])
						conf.VoterConstraints = append(conf.VoterConstraints, cc)
					case "lease-preference":
						cc := parseConstraints(parts[1:])
						conf.LeasePreferences = append(conf.LeasePreferences, roachpb.LeasePreference{
							Constraints: cc,
						})
					default:
						return fmt.Sprintf("unknown field: %s", parts[0])
					}
				}
				var b strings.Builder
				fmt.Fprintf(&b, "input:\n")
				printSpanConf(&b, conf)
				nConf, err := makeNormalizedSpanConfig(&conf, interner)
				if err != nil {
					fmt.Fprintf(&b, "err=%s\n", err.Error())
				}
				if nConf != nil {
					fmt.Fprintf(&b, "output:\n")
					printSpanConf(&b, nConf.uninternedConfig())
				}
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestStoreIDPostingList(t *testing.T) {
	pls := map[string]storeIDPostingList{}
	printPL := func(b *strings.Builder, pl storeIDPostingList) {
		for i := range pl {
			prefix := ""
			if i > 0 {
				prefix = ", "
			}
			fmt.Fprintf(b, "%s%d", prefix, pl[i])
		}
	}
	forceAllocation := rand.Intn(2) == 1

	datadriven.RunTest(t, "testdata/posting_list",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "pl":
				var name string
				d.ScanArgs(t, "name", &name)
				var storeIDs []roachpb.StoreID
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					for _, part := range parts {
						storeID, err := strconv.Atoi(part)
						require.NoError(t, err)
						storeIDs = append(storeIDs, roachpb.StoreID(storeID))
					}
				}
				pl := makeStoreIDPostingList(storeIDs)
				if forceAllocation {
					pl = pl[:len(pl):len(pl)]
				}
				pls[name] = pl
				var b strings.Builder
				printPL(&b, pl)
				return b.String()

			case "intersect", "union", "is-equal":
				var x, y string
				d.ScanArgs(t, "x", &x)
				d.ScanArgs(t, "y", &y)
				plX := pls[x]
				if d.Cmd == "is-equal" {
					return fmt.Sprintf("%t", plX.isEqual(pls[y]))
				} else {
					if d.Cmd == "union" {
						plX.union(pls[y])
					} else if d.Cmd == "intersect" {
						plX.intersect(pls[y])
					}
					if forceAllocation {
						plX = plX[:len(plX):len(plX)]
					}
					pls[x] = plX
					var b strings.Builder
					printPL(&b, plX)
					return b.String()
				}

			case "insert", "contains", "remove":
				var name string
				d.ScanArgs(t, "name", &name)
				pl := pls[name]
				var storeID int
				d.ScanArgs(t, "store-id", &storeID)
				if d.Cmd == "contains" {
					return fmt.Sprintf("%t", pl.contains(roachpb.StoreID(storeID)))
				} else {
					var rv bool
					if d.Cmd == "insert" {
						rv = pl.insert(roachpb.StoreID(storeID))
					} else {
						rv = pl.remove(roachpb.StoreID(storeID))
					}
					if forceAllocation {
						pl = pl[:len(pl):len(pl)]
					}
					pls[name] = pl
					var b strings.Builder
					fmt.Fprintf(&b, "%t: ", rv)
					printPL(&b, pl)
					return b.String()
				}

			case "hash":
				var name string
				d.ScanArgs(t, "name", &name)
				pl := pls[name]
				return fmt.Sprintf("%d", pl.hash())

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
