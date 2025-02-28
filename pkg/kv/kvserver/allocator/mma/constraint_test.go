// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TODO: tests for
// - rangeAnalyzedConstraints initialization: pool and release; stateForInit, finishInit
// - rangeAnalyzedConstraints read-only functions.

func parseConstraint(t *testing.T, field string) roachpb.Constraint {
	var typ roachpb.Constraint_Type
	switch field[0] {
	case '+':
		typ = roachpb.Constraint_REQUIRED
	case '-':
		typ = roachpb.Constraint_PROHIBITED
	default:
		t.Fatalf("unexpected start of field %s", field)
	}
	kv := strings.Split(field[1:], "=")
	if len(kv) != 2 {
		t.Fatalf("unexpected field %s", field)
	}
	return roachpb.Constraint{
		Type:  typ,
		Key:   kv[0],
		Value: kv[1],
	}
}

func parseConstraints(t *testing.T, fields []string) []roachpb.Constraint {
	var cc []roachpb.Constraint
	for _, field := range fields {
		cc = append(cc, parseConstraint(t, field))
	}
	return cc
}

func parseConstraintsConj(t *testing.T, fields []string) roachpb.ConstraintsConjunction {
	var cc roachpb.ConstraintsConjunction
	if strings.HasPrefix(fields[0], "num-replicas=") {
		val := strings.TrimPrefix(fields[0], "num-replicas=")
		replicas, err := strconv.Atoi(val)
		require.NoError(t, err)
		cc.NumReplicas = int32(replicas)
		fields = fields[1:]
	}
	cc.Constraints = parseConstraints(t, fields)
	return cc
}

func parseSpanConfig(t *testing.T, d *datadriven.TestData) roachpb.SpanConfig {
	var numReplicas, numVoters int
	var conf roachpb.SpanConfig
	d.ScanArgs(t, "num-replicas", &numReplicas)
	conf.NumReplicas = int32(numReplicas)
	d.ScanArgs(t, "num-voters", &numVoters)
	conf.NumVoters = int32(numVoters)
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "constraint":
			cc := parseConstraintsConj(t, parts[1:])
			conf.Constraints = append(conf.Constraints, cc)
		case "voter-constraint":
			cc := parseConstraintsConj(t, parts[1:])
			conf.VoterConstraints = append(conf.VoterConstraints, cc)
		case "lease-preference":
			cc := parseConstraints(t, parts[1:])
			conf.LeasePreferences = append(conf.LeasePreferences, roachpb.LeasePreference{
				Constraints: cc,
			})
		default:
			t.Fatalf("unknown field: %s", parts[0])
		}
	}
	return conf
}

func printSpanConfig(b *strings.Builder, conf roachpb.SpanConfig) {
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

func TestNormalizedSpanConfig(t *testing.T) {
	interner := newStringInterner()
	datadriven.RunTest(t, "testdata/normalize_config",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "normalize":
				conf := parseSpanConfig(t, d)
				var b strings.Builder
				fmt.Fprintf(&b, "input:\n")
				printSpanConfig(&b, conf)
				nConf, err := makeNormalizedSpanConfig(&conf, interner)
				if err != nil {
					fmt.Fprintf(&b, "err=%s\n", err.Error())
				}
				if nConf != nil {
					fmt.Fprintf(&b, "output:\n")
					printSpanConfig(&b, nConf.uninternedConfig())
				}
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func printPostingList(b *strings.Builder, pl storeIDPostingList) {
	for i := range pl {
		prefix := ""
		if i > 0 {
			prefix = ", "
		}
		fmt.Fprintf(b, "%s%d", prefix, pl[i])
	}
}

func TestStoreIDPostingList(t *testing.T) {
	pls := map[string]storeIDPostingList{}
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
				printPostingList(&b, pl)
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
					printPostingList(&b, plX)
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
					printPostingList(&b, pl)
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

func parseReplicaType(val string) (roachpb.ReplicaType, error) {
	typ, ok := roachpb.ReplicaType_value[val]
	if !ok {
		return 0, errors.AssertionFailedf("unknown replica type %s", val)
	}
	return roachpb.ReplicaType(typ), nil
}

func leasePrefIndexStr(index int32) string {
	if index == math.MaxInt32 {
		return "none"
	}
	return fmt.Sprintf("%d", index)
}

func printRangeAnalyzedConstraints(
	b *strings.Builder, rac *rangeAnalyzedConstraints, lti *localityTierInterner,
) {
	fmt.Fprintf(b, "needed: voters %d non-voters %d\n",
		rac.numNeededReplicas[voterIndex], rac.numNeededReplicas[nonVoterIndex])
	printStoreAndLocality := func(prefix string, sAndL []storeAndLocality) {
		fmt.Fprintf(b, "%s", prefix)
		for _, elem := range sAndL {
			fmt.Fprintf(b, " %s(%s)", elem.StoreID.String(), lti.unintern(elem.localityTiers).String())
		}
		fmt.Fprintf(b, "\n")
	}
	printStoreIDs := func(prefix string, ids []roachpb.StoreID) {
		fmt.Fprintf(b, "%s", prefix)
		for _, id := range ids {
			fmt.Fprintf(b, " %s", id.String())
		}
		fmt.Fprintf(b, "\n")
	}
	printAnalyzedConstraints := func(prefix string, ac analyzedConstraints) {
		fmt.Fprintf(b, "%s\n", prefix)
		for i := range ac.constraints {
			fmt.Fprintf(b, "  %s\n", ac.constraints[i].unintern(lti.si))
			printStoreIDs("    voters:", ac.satisfiedByReplica[voterIndex][i])
			printStoreIDs("    non-voters:", ac.satisfiedByReplica[nonVoterIndex][i])
		}
		if len(ac.satisfiedNoConstraintReplica[voterIndex])+
			len(ac.satisfiedNoConstraintReplica[nonVoterIndex]) > 0 {
			fmt.Fprintf(b, "  satisfied-no-contraint:\n")
			printStoreIDs("    voters:", ac.satisfiedNoConstraintReplica[voterIndex])
			printStoreIDs("    voters:", ac.satisfiedNoConstraintReplica[nonVoterIndex])
		}
	}
	printStoreAndLocality("voters:", rac.replicas[voterIndex])
	printStoreAndLocality("non-voters:", rac.replicas[nonVoterIndex])
	if !rac.constraints.isEmpty() {
		printAnalyzedConstraints("constraints:", rac.constraints)
	}
	if !rac.voterConstraints.isEmpty() {
		printAnalyzedConstraints("voter-constraints:", rac.voterConstraints)
	}

	fmt.Fprintf(b, "leaseholder pref-index s%d:%s\n", rac.leaseholderID,
		leasePrefIndexStr(rac.leaseholderPreferenceIndex))
	fmt.Fprintf(b, "lease pref-indices:")
	for i := range rac.leasePreferenceIndices {
		fmt.Fprintf(b, " s%d:%s",
			rac.replicas[voterIndex][i].StoreID, leasePrefIndexStr(rac.leasePreferenceIndices[i]))
	}
	fmt.Fprintf(b, "\n")
	fmt.Fprintf(b, "diversity: voter %f, all %f",
		rac.votersDiversityScore, rac.replicasDiversityScore)
}

func testingAnalyzeFn(
	rac *rangeAnalyzedConstraints, fn string, store roachpb.StoreID,
) (toRemove []roachpb.StoreID, toAdd constraintsDisj, err error) {
	switch fn {
	case "nonVoterToVoter":
		toRemove, err = rac.candidatesToConvertFromNonVoterToVoter()
	case "addingVoter":
		toAdd, err = rac.constraintsForAddingVoter()
	case "addingNonVoter":
		toAdd, err = rac.constraintsForAddingNonVoter()
	case "voterToNonVoter":
		toRemove, err = rac.candidatesToConvertFromVoterToNonVoter()
	case "roleSwap":
		var toSwap [numReplicaKinds][]roachpb.StoreID
		toSwap, err = rac.candidatesForRoleSwapForConstraints()
		toRemove = append(toRemove, toSwap[voterIndex]...)
		toRemove = append(toRemove, toSwap[nonVoterIndex]...)
	case "toRemove":
		toRemove, err = rac.candidatesToRemove()
	case "voterUnsatisfied":
		toRemove, toAdd, err = rac.candidatesVoterConstraintsUnsatisfied()
	case "nonVoterUnsatisfied":
		toRemove, toAdd, err = rac.candidatesNonVoterConstraintsUnsatisfied()
	case "replaceVoterRebalance":
		var toAddConj constraintsConj
		toAddConj, err = rac.candidatesToReplaceVoterForRebalance(store)
		toAdd = constraintsDisj{toAddConj}
	case "replaceNonVoterRebalance":
		var toAddConj constraintsConj
		toAddConj, err = rac.candidatesToReplaceNonVoterForRebalance(store)
		toAdd = constraintsDisj{toAddConj}
	default:
		panic("unknown candidate function " + fn)
	}
	return toRemove, toAdd, err
}

// TODO(sumeer): testing of query methods.
func TestRangeAnalyzedConstraints(t *testing.T) {
	interner := newStringInterner()
	cm := newConstraintMatcher(interner)
	ltInterner := newLocalityTierInterner(interner)
	configs := map[string]*normalizedSpanConfig{}
	stores := map[roachpb.StoreID]roachpb.StoreDescriptor{}
	var lastRangeAnalyzedConstraints *rangeAnalyzedConstraints

	datadriven.RunTest(t, "testdata/range_analyzed_constraints",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "store":
				desc := parseStoreDescriptor(t, d)
				cm.setStore(desc)
				stores[desc.StoreID] = desc
				return ""

			case "span-config":
				var name string
				d.ScanArgs(t, "name", &name)
				conf := parseSpanConfig(t, d)
				var b strings.Builder
				nConf, err := makeNormalizedSpanConfig(&conf, interner)
				if err != nil {
					fmt.Fprintf(&b, "normalization error: %s\n", err.Error())
				}
				configs[name] = nConf
				printSpanConfig(&b, nConf.uninternedConfig())
				return b.String()

			case "analyze-constraints":
				var configName string
				d.ScanArgs(t, "config-name", &configName)
				var leaseholder int
				d.ScanArgs(t, "leaseholder", &leaseholder)
				nConf := configs[configName]
				rac := rangeAnalyzedConstraintsPool.Get().(*rangeAnalyzedConstraints)
				buf := rac.stateForInit()
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					var storeID int
					var typ roachpb.ReplicaType
					var err error
					for _, part := range parts {
						if strings.HasPrefix(part, "store-id=") {
							part = strings.TrimPrefix(part, "store-id=")
							storeID, err = strconv.Atoi(part)
							require.NoError(t, err)
						} else if strings.HasPrefix(part, "type=") {
							part = strings.TrimPrefix(part, "type=")
							typ, err = parseReplicaType(part)
							require.NoError(t, err)
						} else {
							t.Fatalf("unknown part %s", part)
						}
					}
					buf.tryAddingStore(roachpb.StoreID(storeID), typ,
						ltInterner.intern(stores[roachpb.StoreID(storeID)].Locality()))
				}
				rac.finishInit(nConf, cm, roachpb.StoreID(leaseholder))
				var b strings.Builder
				printRangeAnalyzedConstraints(&b, rac, ltInterner)
				// If there is a previous rangeAnalyzedConstraints, release it before
				// assigning the rangeAnalyzedConstraints just added as the last.
				if lastRangeAnalyzedConstraints != nil {
					releaseRangeAnalyzedConstraints(lastRangeAnalyzedConstraints)
				}
				lastRangeAnalyzedConstraints = rac
				return b.String()

			case "candidates":
				if lastRangeAnalyzedConstraints == nil {
					return "error: cannot evaluate nil analyzed constraints"
				}
				rac := lastRangeAnalyzedConstraints

				candidateFns := []string{
					"nonVoterToVoter",
					"addingVoter",
					"voterToNonVoter",
					"addingNonVoter",
					"roleSwap",
					"toRemove",
					"voterUnsatisfied",
					"nonVoterUnsatisfied",
					"replaceVoterRebalance",
					"replaceNonVoterRebalance",
				}

				var voterStores, nonVoterStores []roachpb.StoreID
				for _, voter := range rac.replicas[voterIndex] {
					voterStores = append(voterStores, voter.StoreID)
				}
				for _, nonVoter := range rac.replicas[nonVoterIndex] {
					nonVoterStores = append(nonVoterStores, nonVoter.StoreID)
				}

				var buf strings.Builder
				for _, fn := range candidateFns {
					var candidateStores []roachpb.StoreID
					if fn == "replaceNonVoterRebalance" {
						candidateStores = nonVoterStores
					} else if fn == "replaceVoterRebalance" {
						candidateStores = voterStores
					} else {
						// Store is ignored for non replace functions.
						candidateStores = []roachpb.StoreID{-1}
					}

					for _, store := range candidateStores {
						toRemove, toAdd, err := testingAnalyzeFn(rac, fn, store)
						fmt.Fprintf(&buf, "%s", fn)
						if store != -1 {
							fmt.Fprintf(&buf, " replace=%d", store)
						}
						fmt.Fprintf(&buf, "\n")

						if err != nil {
							fmt.Fprintf(&buf, "\terr: %s\n", err.Error())
							continue
						}
						if toRemove != nil {
							fmt.Fprintf(&buf, "\tremove:")
							for _, storeID := range toRemove {
								fmt.Fprintf(&buf, " %d", storeID)
							}
							fmt.Fprintf(&buf, "\n")
						}
						if toAdd != nil {
							fmt.Fprintf(&buf, "\tadd:")
							for _, conj := range toAdd {
								fmt.Fprintf(&buf, " (")
								for i, c := range conj {
									if i > 0 {
										buf.WriteString(",")
									}
									fmt.Fprintf(&buf, "%s", c.unintern(ltInterner.si))
								}
								fmt.Fprintf(&buf, ")")
							}
							fmt.Fprintf(&buf, "\n")
						}
					}
				}
				cands, leaseholderPrefIndex := rac.candidatesToMoveLease()
				fmt.Fprintf(&buf, "toMoveLease\n")
				fmt.Fprintf(&buf, "  leaseholder-pref-index: %s cands:",
					leasePrefIndexStr(leaseholderPrefIndex))
				for _, c := range cands {
					fmt.Fprintf(&buf, " s%d:%s", c.storeID, leasePrefIndexStr(c.leasePreferenceIndex))
				}
				fmt.Fprintf(&buf, "\n")
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
