// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package partition

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

const regionKey = "region"

// Prefix contains a PARTITION BY LIST Prefix, a boolean indicating
// whether the Prefix is from a local partition, and the name of the partition.
type Prefix struct {
	Prefix  tree.Datums
	IsLocal bool

	partitionName string
}

// String returns a string representation of the Prefix, e.g.
// Span prefix with one key in the local region: "[(3), local]"
// Span prefix with two keys and includes remote ranges: "[(0, -1), remote]"
func (pr Prefix) String() string {
	var buf bytes.Buffer
	buf.WriteRune('[')
	buf.WriteString(pr.Prefix.String())
	buf.WriteString(", ")
	if pr.IsLocal {
		buf.WriteString("local")
	} else {
		buf.WriteString("remote")
	}
	buf.WriteRune(']')

	return buf.String()
}

// PrefixSorter sorts prefixes (which are wrapped in Prefix structs) so
// that longer prefixes are ordered first and within each group of equal-length
// prefixes so that they are ordered by value.
type PrefixSorter struct {
	EvalCtx *tree.EvalContext
	Entry   []Prefix

	// A slice of indices of the last element of each equal-length group of
	// entries in the Entry array above. Used by Slice(i int)
	idx []int

	// The set of ordinal numbers indexing into the Entry slice, representing
	// which Prefixes (partitions) are 100% local to the gateway region
	LocalPartitions util.FastIntSet
}

// String returns a string representation of the PrefixSorter.
func (ps PrefixSorter) String() string {
	var buf bytes.Buffer

	for i, prefix := range ps.Entry {
		buf.WriteString(prefix.String())
		if i != len(ps.Entry)-1 {
			buf.WriteRune(' ')
		}
	}

	return buf.String()
}

var _ sort.Interface = &PrefixSorter{}

// Len is part of sort.Interface.
func (ps PrefixSorter) Len() int {
	return len(ps.Entry)
}

// Less is part of sort.Interface.
func (ps PrefixSorter) Less(i, j int) bool {
	if len(ps.Entry[i].Prefix) != len(ps.Entry[j].Prefix) {
		return len(ps.Entry[i].Prefix) > len(ps.Entry[j].Prefix)
	}
	// A zero length prefix is never less than any prefix.
	if len(ps.Entry[i].Prefix) == 0 {
		return false
	}
	compareResult := ps.Entry[i].Prefix.Compare(ps.EvalCtx, ps.Entry[j].Prefix)
	return compareResult == -1
}

// Swap is part of sort.Interface.
func (ps PrefixSorter) Swap(i, j int) {
	ps.Entry[i], ps.Entry[j] = ps.Entry[j], ps.Entry[i]
}

// Slice returns the ith slice of Prefix entries, all having the same
// partition prefix length, along with the start index of that slice in the
// main PrefixSorter Entry slice. Slices are sorted by prefix length with those
// of the longest prefix length occurring at i==0.
func (ps PrefixSorter) Slice(i int) (prefixSlice []Prefix, sliceStartIndex int, ok bool) {
	if i < 0 || i >= len(ps.idx) {
		return nil, -1, false
	}
	inclusiveStartIndex := 0
	if i > 0 {
		// The start of the slice is the end of the previous slice plus one.
		inclusiveStartIndex = ps.idx[i-1] + 1
	}
	nonInclusiveEndIndex := ps.idx[i] + 1
	if (nonInclusiveEndIndex < inclusiveStartIndex) || (nonInclusiveEndIndex > len(ps.Entry)) {
		panic(errors.AssertionFailedf("Partition prefix slice not found. inclusiveStartIndex = %d, nonInclusiveEndIndex = %d",
			inclusiveStartIndex, nonInclusiveEndIndex))
	}
	return ps.Entry[inclusiveStartIndex:nonInclusiveEndIndex], inclusiveStartIndex, true
}

// PrefixesToString prints the string representation of a slice of Prefixes
func PrefixesToString(prefixes []Prefix) string {
	var buf bytes.Buffer
	for i, prefix := range prefixes {
		buf.WriteString(prefix.Prefix.String())
		if i != len(prefixes)-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

// HasMixOfLocalAndRemotePartitions tests if the given index has at least one
// local and one remote partition as used in the current evaluation context.
// This function also returns the set of local partitions when the number of
// partitions in the index is 2 or greater and the local gateway region can be
// determined.
func HasMixOfLocalAndRemotePartitions(
	evalCtx *tree.EvalContext, index cat.Index,
) (localPartitions *util.FastIntSet, ok bool) {
	if index.PartitionCount() < 2 {
		return nil, false
	}
	var localRegion string
	if localRegion, ok = evalCtx.GetLocalRegion(); !ok {
		return nil, false
	}
	var foundLocal, foundRemote bool
	localPartitions = &util.FastIntSet{}
	for i, n := 0, index.PartitionCount(); i < n; i++ {
		part := index.Partition(i)
		if IsZoneLocal(part.Zone(), localRegion) {
			foundLocal = true
			localPartitions.Add(i)
		} else {
			foundRemote = true
		}
	}
	return localPartitions, foundLocal && foundRemote
}

// GetSortedPrefixes collects all the prefixes from all the different partitions
// in the index (remembering which ones came from local partitions), and sorts
// them so that longer prefixes come before shorter prefixes and within each
// group of equal-length prefixes they are ordered by value.
// This is the main function for building a PrefixSorter.
func GetSortedPrefixes(
	index cat.Index, localPartitions util.FastIntSet, evalCtx *tree.EvalContext,
) *PrefixSorter {
	if index == nil || index.PartitionCount() < 2 {
		return nil
	}
	allPrefixes := make([]Prefix, 0, index.PartitionCount())

	for i, n := 0, index.PartitionCount(); i < n; i++ {
		part := index.Partition(i)
		isLocal := localPartitions.Contains(i)
		partitionPrefixes := part.PartitionByListPrefixes()
		if len(partitionPrefixes) == 0 {
			// This can happen when the partition value is DEFAULT.
			allPrefixes = append(allPrefixes, Prefix{
				Prefix:        nil,
				IsLocal:       isLocal,
				partitionName: part.Name(),
			})
		}
		for j := range partitionPrefixes {
			allPrefixes = append(allPrefixes, Prefix{
				Prefix:        partitionPrefixes[j],
				IsLocal:       isLocal,
				partitionName: part.Name(),
			})
		}
	}
	ps := PrefixSorter{evalCtx, allPrefixes, []int{}, localPartitions}
	sort.Sort(ps)
	lastPrefixLength := len(ps.Entry[0].Prefix)
	// Mark the index of each prefix group of a different length.
	// We must search each group separately, so the boundaries need tagging.
	for i := 1; i < len(allPrefixes); i++ {
		if len(ps.Entry[i].Prefix) != lastPrefixLength {
			ps.idx = append(ps.idx, i-1)
			lastPrefixLength = len(ps.Entry[i].Prefix)
		}
	}
	ps.LocalPartitions = localPartitions

	// The end of the last slice is always the last element.
	ps.idx = append(ps.idx, len(allPrefixes)-1)
	return &ps
}

// isConstraintLocal returns isLocal=true and ok=true if the given constraint is
// a required constraint matching the given localRegion. Returns isLocal=false
// and ok=true if the given constraint is a prohibited constraint matching the
// given local region or if it is a required constraint matching a different
// region. Any other scenario returns ok=false, since this constraint gives no
// information about whether the constrained replicas are local or remote.
func isConstraintLocal(constraint cat.Constraint, localRegion string) (isLocal bool, ok bool) {
	if constraint.GetKey() != regionKey {
		// We only care about constraints on the region.
		return false /* isLocal */, false /* ok */
	}
	if constraint.GetValue() == localRegion {
		if constraint.IsRequired() {
			// The local region is required.
			return true /* isLocal */, true /* ok */
		}
		// The local region is prohibited.
		return false /* isLocal */, true /* ok */
	}
	if constraint.IsRequired() {
		// A remote region is required.
		return false /* isLocal */, true /* ok */
	}
	// A remote region is prohibited, so this constraint gives no information
	// about whether the constrained replicas are local or remote.
	return false /* isLocal */, false /* ok */
}

// IsZoneLocal returns true if the given zone config indicates that the replicas
// it constrains will be primarily located in the localRegion.
func IsZoneLocal(zone cat.Zone, localRegion string) bool {
	// First count the number of local and remote replica constraints. If all
	// are local or all are remote, we can return early.
	local, remote := 0, 0
	for i, n := 0, zone.ReplicaConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.ReplicaConstraints(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				if isLocal {
					local++
				} else {
					remote++
				}
			}
		}
	}
	if local > 0 && remote == 0 {
		return true
	}
	if remote > 0 && local == 0 {
		return false
	}

	// Next check the voter replica constraints. Once again, if all are local or
	// all are remote, we can return early.
	local, remote = 0, 0
	for i, n := 0, zone.VoterConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.VoterConstraint(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				if isLocal {
					local++
				} else {
					remote++
				}
			}
		}
	}
	if local > 0 && remote == 0 {
		return true
	}
	if remote > 0 && local == 0 {
		return false
	}

	// Use the lease preferences as a tie breaker. We only really care about the
	// first one, since subsequent lease preferences only apply in edge cases.
	if zone.LeasePreferenceCount() > 0 {
		leasePref := zone.LeasePreference(0)
		for i, n := 0, leasePref.ConstraintCount(); i < n; i++ {
			constraint := leasePref.Constraint(i)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				return isLocal
			}
		}
	}

	return false
}
