// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// NodeID is a custom type for a cockroach node ID. (not a raft node ID)
// 0 is not a valid NodeID.
type NodeID int32

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n NodeID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// SafeValue implements the redact.SafeValue interface.
func (n NodeID) SafeValue() {}

// NodeIDSlice implements sort.Interface.
type NodeIDSlice []NodeID

func (s NodeIDSlice) Len() int           { return len(s) }
func (s NodeIDSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s NodeIDSlice) Less(i, j int) bool { return s[i] < s[j] }

func (s NodeIDSlice) String() string {
	var sb strings.Builder
	for i, ni := range s {
		if i > 0 {
			sb.WriteRune(',')
		}
		fmt.Fprintf(&sb, "n%d", ni)
	}
	return sb.String()
}

// StoreID is a custom type for a cockroach store ID.
type StoreID int32

// StoreIDSlice implements sort.Interface.
type StoreIDSlice []StoreID

func (s StoreIDSlice) Len() int           { return len(s) }
func (s StoreIDSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StoreIDSlice) Less(i, j int) bool { return s[i] < s[j] }

func (s StoreIDSlice) String() string {
	var sb strings.Builder
	for i, st := range s {
		if i > 0 {
			sb.WriteRune(',')
		}
		fmt.Fprintf(&sb, "s%d", st)
	}
	return sb.String()
}

// SafeValue implements the redact.SafeValue interface.
func (s StoreIDSlice) SafeValue() {}

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n StoreID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// SafeValue implements the redact.SafeValue interface.
func (n StoreID) SafeValue() {}

// A RangeID is a unique ID associated to a Raft consensus group.
type RangeID int64

// String implements the fmt.Stringer interface.
func (r RangeID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// SafeValue implements the redact.SafeValue interface.
func (r RangeID) SafeValue() {}

// ReplicaID is a custom type for a range replica ID.
type ReplicaID int32

// String implements the fmt.Stringer interface.
func (r ReplicaID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// SafeValue implements the redact.SafeValue interface.
func (r ReplicaID) SafeValue() {}

// Equals returns whether the Attributes lists are equivalent. Attributes lists
// are treated as sets, meaning that ordering and duplicates are ignored.
func (a Attributes) Equals(b Attributes) bool {
	// This is O(n^2), but Attribute lists should never be long enough for that
	// to matter, and allocating memory every time this is called would be worse.
	if len(a.Attrs) != len(b.Attrs) {
		return false
	}
	for _, aAttr := range a.Attrs {
		var found bool
		for _, bAttr := range b.Attrs {
			if aAttr == bAttr {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// String implements the fmt.Stringer interface.
func (a Attributes) String() string {
	return strings.Join(a.Attrs, ",")
}

// RangeGeneration is a custom type for a range generation. Range generation
// counters are incremented on every split, merge, and every replica change,
// i.e., whenever the span of the range or replica set changes.
//
// See the comment on RangeDescriptor.Generation for more.
type RangeGeneration int64

// String implements the fmt.Stringer interface.
func (g RangeGeneration) String() string {
	return strconv.FormatInt(int64(g), 10)
}

// SafeValue implements the redact.SafeValue interface.
func (g RangeGeneration) SafeValue() {}

// NewRangeDescriptor returns a RangeDescriptor populated from the input.
func NewRangeDescriptor(rangeID RangeID, start, end RKey, replicas ReplicaSet) *RangeDescriptor {
	repls := append([]ReplicaDescriptor(nil), replicas.Descriptors()...)
	for i := range repls {
		repls[i].ReplicaID = ReplicaID(i + 1)
	}
	desc := &RangeDescriptor{
		RangeID:       rangeID,
		StartKey:      start,
		EndKey:        end,
		NextReplicaID: ReplicaID(len(repls) + 1),
	}
	desc.SetReplicas(MakeReplicaSet(repls))
	return desc
}

// GetRangeID returns the RangeDescriptor's ID.
// The method implements the batcheval.ImmutableRangeState interface.
func (r *RangeDescriptor) GetRangeID() RangeID {
	return r.RangeID
}

// GetStartKey returns the RangeDescriptor's start key.
// The method implements the batcheval.ImmutableRangeState interface.
func (r *RangeDescriptor) GetStartKey() RKey {
	return r.StartKey
}

// RSpan returns the RangeDescriptor's resolved span.
func (r *RangeDescriptor) RSpan() RSpan {
	return RSpan{Key: r.StartKey, EndKey: r.EndKey}
}

// KeySpan returns the keys covered by this range. Local keys are not included.
// This is identical to RSpan(), but for r1 the StartKey is forwarded to LocalMax.
//
// See: https://github.com/cockroachdb/cockroach/issues/95055
func (r *RangeDescriptor) KeySpan() RSpan {
	return r.RSpan().KeySpan()
}

// ContainsKey returns whether this RangeDescriptor contains the specified key.
func (r *RangeDescriptor) ContainsKey(key RKey) bool {
	return r.RSpan().ContainsKey(key)
}

// ContainsKeyInverted returns whether this RangeDescriptor contains the
// specified key using an inverted range. See RSpan.ContainsKeyInverted.
func (r *RangeDescriptor) ContainsKeyInverted(key RKey) bool {
	return r.RSpan().ContainsKeyInverted(key)
}

// ContainsKeyRange returns whether this RangeDescriptor contains the specified
// key range from start (inclusive) to end (exclusive).
// If end is empty, returns ContainsKey(start).
func (r *RangeDescriptor) ContainsKeyRange(start, end RKey) bool {
	return r.RSpan().ContainsKeyRange(start, end)
}

// Replicas returns the set of nodes/stores on which replicas of this range are
// stored.
func (r *RangeDescriptor) Replicas() ReplicaSet {
	return MakeReplicaSet(r.InternalReplicas)
}

// SetReplicas overwrites the set of nodes/stores on which replicas of this
// range are stored.
func (r *RangeDescriptor) SetReplicas(replicas ReplicaSet) {
	r.InternalReplicas = replicas.Descriptors()
}

// SetReplicaType changes the type of the replica with the given ID to the given
// type. Returns zero values if the replica was not found and the updated
// descriptor, the previous type, and true, otherwise.
func (r *RangeDescriptor) SetReplicaType(
	nodeID NodeID, storeID StoreID, typ ReplicaType,
) (ReplicaDescriptor, ReplicaType, bool) {
	for i := range r.InternalReplicas {
		desc := &r.InternalReplicas[i]
		if desc.StoreID == storeID && desc.NodeID == nodeID {
			prevTyp := desc.Type
			desc.Type = typ
			return *desc, prevTyp, true
		}
	}
	return ReplicaDescriptor{}, 0, false
}

// AddReplica adds a replica on the given node and store with the supplied type.
// It auto-assigns a ReplicaID and returns the inserted ReplicaDescriptor.
func (r *RangeDescriptor) AddReplica(
	nodeID NodeID, storeID StoreID, typ ReplicaType,
) ReplicaDescriptor {
	toAdd := ReplicaDescriptor{
		NodeID:    nodeID,
		StoreID:   storeID,
		ReplicaID: r.NextReplicaID,
		Type:      typ,
	}
	rs := r.Replicas()
	rs.AddReplica(toAdd)
	r.SetReplicas(rs)
	r.NextReplicaID++
	return toAdd
}

// RemoveReplica removes the matching replica from this range's set and returns
// it. If it wasn't found to remove, false is returned.
func (r *RangeDescriptor) RemoveReplica(nodeID NodeID, storeID StoreID) (ReplicaDescriptor, bool) {
	rs := r.Replicas()
	removedRepl, ok := rs.RemoveReplica(nodeID, storeID)
	if ok {
		r.SetReplicas(rs)
	}
	return removedRepl, ok
}

// GetReplicaDescriptor returns the replica which matches the specified store
// ID.
func (r *RangeDescriptor) GetReplicaDescriptor(storeID StoreID) (ReplicaDescriptor, bool) {
	for _, repDesc := range r.Replicas().Descriptors() {
		if repDesc.StoreID == storeID {
			return repDesc, true
		}
	}
	return ReplicaDescriptor{}, false
}

// GetReplicaDescriptorByID returns the replica which matches the specified
// replica ID.
func (r *RangeDescriptor) GetReplicaDescriptorByID(replicaID ReplicaID) (ReplicaDescriptor, bool) {
	return r.Replicas().GetReplicaDescriptorByID(replicaID)
}

// IsInitialized returns false if this descriptor represents an
// uninitialized range.
// TODO(bdarnell): unify this with Validate().
func (r *RangeDescriptor) IsInitialized() bool {
	return len(r.EndKey) != 0
}

// IncrementGeneration increments the generation of this RangeDescriptor.
// This method mutates the receiver; do not call it with shared RangeDescriptors.
func (r *RangeDescriptor) IncrementGeneration() {
	r.Generation++
}

// Validate performs some basic validation of the contents of a range descriptor.
func (r *RangeDescriptor) Validate() error {
	if r.NextReplicaID == 0 {
		return errors.Errorf("NextReplicaID must be non-zero")
	}
	seen := map[ReplicaID]struct{}{}
	stores := map[StoreID]struct{}{}
	for i, rep := range r.Replicas().Descriptors() {
		if err := rep.Validate(); err != nil {
			return errors.Wrapf(err, "replica %d is invalid", i)
		}
		if rep.ReplicaID >= r.NextReplicaID {
			return errors.Errorf("ReplicaID %d must be less than NextReplicaID %d",
				rep.ReplicaID, r.NextReplicaID)
		}

		if _, ok := seen[rep.ReplicaID]; ok {
			return errors.Errorf("ReplicaID %d was reused", rep.ReplicaID)
		}
		seen[rep.ReplicaID] = struct{}{}

		if _, ok := stores[rep.StoreID]; ok {
			return errors.Errorf("StoreID %d was reused", rep.StoreID)
		}
		stores[rep.StoreID] = struct{}{}
	}
	return nil
}

func (r RangeDescriptor) String() string {
	return redact.StringWithoutMarkers(r)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r RangeDescriptor) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%d:", r.RangeID)
	if !r.IsInitialized() {
		w.SafeString("{-}")
	} else {
		w.Print(r.RSpan())
	}
	w.SafeString(" [")

	if allReplicas := r.Replicas().Descriptors(); len(allReplicas) > 0 {
		for i, rep := range allReplicas {
			if i > 0 {
				w.SafeString(", ")
			}
			w.Print(rep)
		}
	} else {
		w.SafeString("<no replicas>")
	}
	w.Printf(", next=%d, gen=%d", r.NextReplicaID, r.Generation)
	if !r.StickyBit.IsEmpty() {
		w.Printf(", sticky=%s", r.StickyBit)
	}
	w.SafeString("]")
}

func (r ReplicationTarget) String() string {
	return redact.StringWithoutMarkers(r)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r ReplicationTarget) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("n%d,s%d", r.NodeID, r.StoreID)
}

func (r ReplicaDescriptor) String() string {
	return redact.StringWithoutMarkers(r)
}

// IsSame returns true if the two replica descriptors refer to the same replica,
// ignoring the replica type.
func (r ReplicaDescriptor) IsSame(o ReplicaDescriptor) bool {
	return r.NodeID == o.NodeID && r.StoreID == o.StoreID && r.ReplicaID == o.ReplicaID
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r ReplicaDescriptor) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(n%d,s%d):", r.NodeID, r.StoreID)
	if r.ReplicaID == 0 {
		w.SafeRune('?')
	} else {
		w.Print(r.ReplicaID)
	}
	if r.Type != VOTER_FULL {
		w.Print(r.Type)
	}
}

// Validate performs some basic validation of the contents of a replica descriptor.
func (r ReplicaDescriptor) Validate() error {
	if r.NodeID == 0 {
		return errors.Errorf("NodeID must not be zero")
	}
	if r.StoreID == 0 {
		return errors.Errorf("StoreID must not be zero")
	}
	if r.ReplicaID == 0 {
		return errors.Errorf("ReplicaID must not be zero")
	}
	return nil
}

// SafeValue implements the redact.SafeValue interface.
func (r ReplicaType) SafeValue() {}

// GetReplicaDescriptorByID returns the replica which matches the specified
// replica ID.
func (r ReplicaSet) GetReplicaDescriptorByID(id ReplicaID) (repDesc ReplicaDescriptor, found bool) {
	for i := range r.wrapped {
		if r.wrapped[i].ReplicaID == id {
			return r.wrapped[i], true
		}
	}
	return ReplicaDescriptor{}, false
}

// IsVoterOldConfig returns true if the replica is a voter in the outgoing
// config (or, simply is a voter if the range is not in a joint-config state).
// Can be used as a filter for
// ReplicaDescriptors.Filter(ReplicaDescriptor.IsVoterOldConfig).
func (r ReplicaDescriptor) IsVoterOldConfig() bool {
	switch r.Type {
	case VOTER_FULL, VOTER_OUTGOING, VOTER_DEMOTING_NON_VOTER, VOTER_DEMOTING_LEARNER:
		return true
	default:
		return false
	}
}

// IsVoterNewConfig returns true if the replica is a voter in the incoming
// config (or, simply is a voter if the range is not in a joint-config state).
// Can be used as a filter for
// ReplicaDescriptors.Filter(ReplicaDescriptor.IsVoterOldConfig).
func (r ReplicaDescriptor) IsVoterNewConfig() bool {
	switch r.Type {
	case VOTER_FULL, VOTER_INCOMING:
		return true
	default:
		return false
	}
}

// IsAnyVoter returns true if the replica is a voter in the previous
// config (pre-reconfiguration) or the incoming config. Can be used as a filter
// for ReplicaDescriptors.Filter(ReplicaDescriptor.IsVoterOldConfig).
func (r ReplicaDescriptor) IsAnyVoter() bool {
	switch r.Type {
	case VOTER_FULL, VOTER_INCOMING, VOTER_OUTGOING, VOTER_DEMOTING_NON_VOTER, VOTER_DEMOTING_LEARNER:
		return true
	default:
		return false
	}
}

// IsNonVoter returns true if the replica is a non-voter. Can be used as a
// filter for ReplicaDescriptors.Filter.
func (r ReplicaDescriptor) IsNonVoter() bool {
	switch r.Type {
	case NON_VOTER:
		return true
	default:
		return false
	}
}

// PercentilesFromData derives percentiles from a slice of data points.
// Sorts the input data if it isn't already sorted.
func PercentilesFromData(data []float64) Percentiles {
	sort.Float64s(data)

	return Percentiles{
		P10:  percentileFromSortedData(data, 10),
		P25:  percentileFromSortedData(data, 25),
		P50:  percentileFromSortedData(data, 50),
		P75:  percentileFromSortedData(data, 75),
		P90:  percentileFromSortedData(data, 90),
		PMax: percentileFromSortedData(data, 100),
	}
}

func percentileFromSortedData(data []float64, percent float64) float64 {
	if len(data) == 0 {
		return 0
	}
	if percent < 0 {
		percent = 0
	}
	if percent >= 100 {
		return data[len(data)-1]
	}
	// TODO(a-robinson): Use go's rounding function once we're using 1.10.
	idx := int(float64(len(data)) * percent / 100.0)
	return data[idx]
}

// String returns a string representation of the Percentiles.
func (p Percentiles) String() string {
	return redact.StringWithoutMarkers(p)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (p Percentiles) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("p10=%.2f p25=%.2f p50=%.2f p75=%.2f p90=%.2f pMax=%.2f",
		p.P10, p.P25, p.P50, p.P75, p.P90, p.PMax)
}

func (sc FileStoreProperties) String() string {
	return redact.StringWithoutMarkers(sc)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (sc FileStoreProperties) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("{path=%s, fs=%s, blkdev=%s, mnt=%s opts=%s}",
		sc.Path,
		redact.SafeString(sc.FsType),
		sc.BlockDevice,
		sc.MountPoint,
		sc.MountOptions)
}

// String returns a string representation of the StoreCapacity.
func (sc StoreCapacity) String() string {
	return redact.StringWithoutMarkers(sc)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (sc StoreCapacity) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("disk (capacity=%s, available=%s, used=%s, logicalBytes=%s), "+
		"ranges=%d, leases=%d, queries=%.2f, writes=%.2f, "+
		"ioThreshold={%v} bytesPerReplica={%s}, writesPerReplica={%s}",
		humanizeutil.IBytes(sc.Capacity), humanizeutil.IBytes(sc.Available),
		humanizeutil.IBytes(sc.Used), humanizeutil.IBytes(sc.LogicalBytes),
		sc.RangeCount, sc.LeaseCount, sc.QueriesPerSecond, sc.WritesPerSecond,
		sc.IOThreshold, sc.BytesPerReplica, sc.WritesPerReplica)
}

// FractionUsed computes the fraction of storage capacity that is in use.
func (sc StoreCapacity) FractionUsed() float64 {
	if sc.Capacity == 0 {
		return 0
	}
	// Prefer computing the fraction of available disk space used by considering
	// anything on the disk that isn't in the store's data directory just a sunk
	// cost, not truly part of the disk's capacity. This means that the disk's
	// capacity is really just the available space plus cockroach's usage.
	//
	// Fall back to a more pessimistic calculation of disk usage if we don't know
	// how much space the store's data is taking up.
	if sc.Used == 0 {
		return float64(sc.Capacity-sc.Available) / float64(sc.Capacity)
	}
	return float64(sc.Used) / float64(sc.Available+sc.Used)
}

// Load returns an allocator load representation of the store capacity.
func (sc StoreCapacity) Load() load.Load {
	dims := load.Vector{}
	dims[load.Queries] = sc.QueriesPerSecond
	dims[load.CPU] = sc.CPUPerSecond
	return dims

}

// AddressForLocality returns the network address that nodes in the specified
// locality should use when connecting to the node described by the descriptor.
func (n *NodeDescriptor) AddressForLocality(loc Locality) *util.UnresolvedAddr {
	// If the provided locality has any tiers that are an exact match (key
	// and value) with a tier in the node descriptor's custom LocalityAddress
	// list, return the corresponding address. Otherwise, return the default
	// address.
	return loc.LookupAddress(n.LocalityAddress, &n.Address)
}

// CheckedSQLAddress returns the value of SQLAddress if set. If not, either
// because the receiver is a pre-19.2 node, or because it is using the same
// address for both SQL and RPC, the Address is returned.
func (n *NodeDescriptor) CheckedSQLAddress() *util.UnresolvedAddr {
	if n.SQLAddress.IsEmpty() {
		return &n.Address
	}
	return &n.SQLAddress
}

// String returns a string representation of the Tier.
func (t Tier) String() string {
	return fmt.Sprintf("%s=%s", t.Key, t.Value)
}

// FromString parses the string representation into the Tier.
func (t *Tier) FromString(tier string) error {
	parts := strings.Split(tier, "=")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return errors.Errorf("tier must be in the form \"key=value\" not %q", tier)
	}
	t.Key = parts[0]
	t.Value = parts[1]
	return nil
}

// String returns a string representation of all the Tiers. This is part
// of pflag's value interface.
func (l Locality) String() string {
	tiers := make([]string, len(l.Tiers))
	for i, tier := range l.Tiers {
		tiers[i] = tier.String()
	}
	return strings.Join(tiers, ",")
}

// Empty returns true if the tiers are empty.
func (l Locality) Empty() bool {
	return len(l.Tiers) == 0
}

// NonEmpty returns true if the tiers are non-empty.
func (l Locality) NonEmpty() bool {
	return len(l.Tiers) > 0
}

// Matches checks if this locality has a tier with a matching value for each
// tier of the passed filter, returning true if so or false if not along with
// the first tier of the filters that did not matched.
func (l Locality) Matches(filter Locality) (bool, Tier) {
	for _, t := range filter.Tiers {
		if v, ok := l.Find(t.Key); !ok || v != t.Value {
			return false, t
		}
	}
	return true, Tier{}
}

// getFirstRegionFirstZone iterates through the locality tiers and returns
// multiple values containing:
// 1. The value of the first encountered "region" tier.
// 2. A boolean indicating whether the "region" tier key was found.
// 3,4. The key and the value of the first encountered "zone" tier.
// 5. A boolean indicating whether the "zone" tier key was found.
func (l Locality) getFirstRegionFirstZone() (
	firstRegionValue string,
	hasRegion bool,
	firstZoneKey string,
	firstZoneValue string,
	hasZone bool,
) {
	for _, tier := range l.Tiers {
		if hasRegion && hasZone {
			break
		}
		switch tier.Key {
		case "region":
			if !hasRegion {
				firstRegionValue = tier.Value
				hasRegion = true
			}
		case "zone", "availability-zone", "az":
			if !hasZone {
				firstZoneKey, firstZoneValue = tier.Key, tier.Value
				hasZone = true
			}
		}
	}
	return firstRegionValue, hasRegion, firstZoneKey, firstZoneValue, hasZone
}

// CompareWithLocality returns the comparison result between this and the
// provided other locality along with any lookup errors. Possible errors include
// 1. if either locality does not have a "region" tier key. 2. if either
// locality does not have a "zone" tier key or if the first "zone" tier keys
// used by two localities are different.
//
// Limitation:
// - It is unfortunate that the tier key is hardcoded here. Ideally, we would
// prefer a more robust way to look up node locality regions and zones.
// - Although it is technically possible for users to use  “az”, “zone”,
// “availability-zone” as tier keys within a single locality, it can cause
// confusion when choosing the zone tier values for cross-zone comparison. In
// such cases, we would want to return an error. Ideally, both localities would
// be checked thoroughly for duplicate zone tier keys and key mismatches.
// However, due to frequent invocation of this function, we prefer to terminate
// the check after examining the first encountered zone tier key-value pairs.
//
// Note: it is intentional here to perform multiple locality tiers comparison in
// a single function to avoid overhead. If you are adding additional locality
// tiers comparisons, it is recommended to handle them within one tier list
// iteration.
func (l Locality) CompareWithLocality(
	other Locality,
) (_ LocalityComparisonType, regionValid bool, zoneValid bool) {
	firstRegionValue, hasRegion, firstZoneKey, firstZone, hasZone := l.getFirstRegionFirstZone()
	firstRegionValueOther, hasRegionOther, firstZoneKeyOther, firstZoneOther, hasZoneOther := other.getFirstRegionFirstZone()

	isCrossRegion := firstRegionValue != firstRegionValueOther
	isCrossZone := firstZone != firstZoneOther

	if !hasRegion || !hasRegionOther {
		isCrossRegion = false
	} else {
		regionValid = true
	}

	if (!hasZone || !hasZoneOther) || (firstZoneKey != firstZoneKeyOther) {
		isCrossZone = false
	} else {
		zoneValid = true
	}

	if isCrossRegion {
		return LocalityComparisonType_CROSS_REGION, regionValid, zoneValid
	} else {
		if isCrossZone {
			return LocalityComparisonType_SAME_REGION_CROSS_ZONE, regionValid, zoneValid
		} else {
			return LocalityComparisonType_SAME_REGION_SAME_ZONE, regionValid, zoneValid
		}
	}
}

// SharedPrefix returns the number of this locality's tiers which match those of
// the passed locality.
func (l Locality) SharedPrefix(other Locality) int {
	for i := range l.Tiers {
		if i >= len(other.Tiers) || l.Tiers[i] != other.Tiers[i] {
			return i
		}
	}
	return len(l.Tiers)
}

// Type returns the underlying type in string form. This is part of pflag's
// value interface.
func (Locality) Type() string {
	return "Locality"
}

// Equals returns whether the two Localities are equivalent.
//
// Because Locality Tiers are hierarchically ordered, if two Localities contain
// the same Tiers in different orders, they are not considered equal.
func (l Locality) Equals(r Locality) bool {
	if len(l.Tiers) != len(r.Tiers) {
		return false
	}
	for i := range l.Tiers {
		if l.Tiers[i] != r.Tiers[i] {
			return false
		}
	}
	return true
}

// LookupAddress is given a set of LocalityAddresses and finds the one that
// exactly matches my Locality. O(n^2), but we expect very few locality tiers in
// practice.
func (l Locality) LookupAddress(
	address []LocalityAddress, base *util.UnresolvedAddr,
) *util.UnresolvedAddr {
	for i := range address {
		nLoc := &address[i]
		for _, loc := range l.Tiers {
			if loc == nLoc.LocalityTier {
				return &nLoc.Address
			}
		}
	}
	return base
}

// MaxDiversityScore is the largest possible diversity score, indicating that
// two localities are as different from each other as possible.
const MaxDiversityScore = 1.0

// DiversityScore returns a score comparing the two localities which ranges from
// 1, meaning completely diverse, to 0 which means not diverse at all (that
// their localities match). This function ignores the locality tier key names
// and only considers differences in their values.
//
// All localities are sorted from most global to most local so any localities
// after any differing values are irrelevant.
//
// While we recommend that all nodes have the same locality keys and same
// total number of keys, there's nothing wrong with having different locality
// keys as long as the immediately next keys are all the same for each value.
// For example:
// region:USA -> state:NY -> ...
// region:USA -> state:WA -> ...
// region:EUR -> country:UK -> ...
// region:EUR -> country:France -> ...
// is perfectly fine. This holds true at each level lower as well.
//
// There is also a need to consider the cases where the localities have
// different lengths. For these cases, we treat the missing key on one side as
// different.
func (l Locality) DiversityScore(other Locality) float64 {
	length := len(l.Tiers)
	if len(other.Tiers) < length {
		length = len(other.Tiers)
	}
	for i := 0; i < length; i++ {
		if l.Tiers[i].Value != other.Tiers[i].Value {
			return float64(length-i) / float64(length)
		}
	}
	if len(l.Tiers) != len(other.Tiers) {
		return MaxDiversityScore / float64(length+1)
	}
	return 0
}

// Set sets the value of the Locality. It is the important part of
// pflag's value interface.
func (l *Locality) Set(value string) error {
	if len(l.Tiers) > 0 {
		return errors.New("can't set locality more than once")
	}
	if len(value) == 0 {
		return errors.New("can't have empty locality")
	}

	tiersStr := strings.Split(value, ",")
	tiers := make([]Tier, len(tiersStr))
	for i, tier := range tiersStr {
		if err := tiers[i].FromString(tier); err != nil {
			return err
		}
	}
	l.Tiers = tiers
	return nil
}

// CopyReplaceKeyValue makes a copy of this locality, replacing any tier in the
// copy having the specified `key` with the new specified `value`.
func (l *Locality) CopyReplaceKeyValue(key, value string) Locality {
	tiers := make([]Tier, len(l.Tiers))
	for i := range l.Tiers {
		tiers[i] = l.Tiers[i]
		if tiers[i].Key == key {
			tiers[i].Value = value
		}
	}
	return Locality{Tiers: tiers}
}

// Find searches the locality's tiers for the input key, returning its value if
// present.
func (l *Locality) Find(key string) (value string, ok bool) {
	for i := range l.Tiers {
		if l.Tiers[i].Key == key {
			return l.Tiers[i].Value, true
		}
	}
	return "", false
}

// DefaultLocationInformation is used to populate the system.locations
// table. The region values here are specific to GCP.
var DefaultLocationInformation = []struct {
	Locality  Locality
	Latitude  string
	Longitude string
}{
	{
		Locality:  Locality{Tiers: []Tier{{Key: "region", Value: "us-east1"}}},
		Latitude:  "33.836082",
		Longitude: "-81.163727",
	},
	{
		Locality:  Locality{Tiers: []Tier{{Key: "region", Value: "us-east4"}}},
		Latitude:  "37.478397",
		Longitude: "-76.453077",
	},
	{
		Locality:  Locality{Tiers: []Tier{{Key: "region", Value: "us-central1"}}},
		Latitude:  "42.032974",
		Longitude: "-93.581543",
	},
	{
		Locality:  Locality{Tiers: []Tier{{Key: "region", Value: "us-west1"}}},
		Latitude:  "43.804133",
		Longitude: "-120.554201",
	},
	{
		Locality:  Locality{Tiers: []Tier{{Key: "region", Value: "europe-west1"}}},
		Latitude:  "50.44816",
		Longitude: "3.81886",
	},
}

// Locality returns the locality of the Store, which is the Locality of the node
// plus an extra tier for the node itself.
func (s StoreDescriptor) Locality() Locality {
	return s.Node.Locality.AddTier(
		Tier{Key: "node", Value: s.Node.NodeID.String()})
}

// AddTier creates a new Locality with a Tier at the end.
func (l Locality) AddTier(tier Tier) Locality {
	if len(l.Tiers) > 0 {
		tiers := make([]Tier, len(l.Tiers), len(l.Tiers)+1)
		copy(tiers, l.Tiers)
		tiers = append(tiers, tier)
		return Locality{Tiers: tiers}
	}
	return Locality{Tiers: []Tier{tier}}
}

// IsEmpty returns true if hint contains no data.
func (h *GCHint) IsEmpty() bool {
	return h.LatestRangeDeleteTimestamp.IsEmpty() && h.GCTimestamp.IsEmpty()
}

// Merge combines GC hints of two adjacent ranges. Updates the receiver to be a
// GCHint that covers both ranges, and so can be carried by the merged range.
// Returns true iff the receiver was updated.
//
// The leftEmpty and rightEmpty arguments correspond to MVCCStats.HasNoUserData
// of the receiver hint and the argument RHS hint respectively. These are used
// by a heuristic, to stop carrying the LatestRangeDeleteTimestamp field of the
// hint it the range is likely not covered by MVCC range tombstones (anymore).
//
// Merge is commutative in a sense that the merged hint does not change if the
// order of the LHS and RHS hints (and leftEmpty/rightEmpty, correspondingly) is
// swapped.
func (h *GCHint) Merge(rhs *GCHint, leftEmpty, rightEmpty bool) bool {
	updated := h.ScheduleGCFor(rhs.GCTimestamp)
	// NB: don't swap the operands, we need the side effect of the method call.
	updated = h.ScheduleGCFor(rhs.GCTimestampNext) || updated

	// If LHS or RHS has data but no LatestRangeDeleteTimestamp hint, then this
	// side is not known to be covered by range tombstones. Correspondingly, the
	// union of the two is not too. If so, clear the hint.
	if (rhs.LatestRangeDeleteTimestamp.IsEmpty() && !rightEmpty) ||
		(h.LatestRangeDeleteTimestamp.IsEmpty() && !leftEmpty) {
		updated = updated || h.LatestRangeDeleteTimestamp.IsSet()
		h.LatestRangeDeleteTimestamp = hlc.Timestamp{}
		return updated
	}
	// TODO(pavelkalinnikov): handle the case when some side has a hint (i.e. is
	// covered by range tombstones), but is not empty. It means that there is data
	// on top of the range tombstones, so the ClearRange optimization may not be
	// effective. For now, live with the false positive because this is unlikely.

	return h.ForwardLatestRangeDeleteTimestamp(rhs.LatestRangeDeleteTimestamp) || updated
}

// ForwardLatestRangeDeleteTimestamp bumps LatestDeleteRangeTimestamp in GC hint
// if it is greater than previously set.
func (h *GCHint) ForwardLatestRangeDeleteTimestamp(ts hlc.Timestamp) bool {
	if h.LatestRangeDeleteTimestamp.Less(ts) {
		h.LatestRangeDeleteTimestamp = ts
		return true
	}
	return false
}

// ScheduleGCFor updates the hint to schedule eager GC for data up to the given
// timestamp. When this timestamp falls below the GC threshold/TTL, it will be
// eagerly enqueued for GC when considered by the MVCC GC queue.
//
// Returns true iff the hint was updated.
func (h *GCHint) ScheduleGCFor(ts hlc.Timestamp) bool {
	if ts.IsEmpty() {
		return false
	}
	if h.GCTimestamp.IsEmpty() {
		h.GCTimestamp = ts
	} else if cmp := h.GCTimestamp.Compare(ts); cmp > 0 {
		if h.GCTimestampNext.IsEmpty() {
			h.GCTimestampNext = h.GCTimestamp
		}
		h.GCTimestamp = ts
	} else if cmp == 0 {
		return false
	} else if h.GCTimestampNext.IsEmpty() {
		h.GCTimestampNext = ts
	} else if ts.LessEq(h.GCTimestampNext) {
		return false
	} else {
		h.GCTimestampNext = ts
	}
	return true
}

// UpdateAfterGC updates the GCHint according to the threshold, up to which the
// data has been garbage collected. Returns true iff the hint has been updated.
func (h *GCHint) UpdateAfterGC(gcThreshold hlc.Timestamp) bool {
	updated := h.advanceGCTimestamp(gcThreshold)
	if t := h.LatestRangeDeleteTimestamp; t.IsSet() && t.LessEq(gcThreshold) {
		h.LatestRangeDeleteTimestamp = hlc.Timestamp{}
		return true
	}
	return updated
}

func (h *GCHint) advanceGCTimestamp(gcThreshold hlc.Timestamp) bool {
	// If GC threshold is below the minimum, leave the hint intact.
	if t := h.GCTimestamp; t.IsEmpty() || gcThreshold.Less(t) {
		return false
	}
	// If min <= threshold < max, erase the min and set it to match the max.
	if t := h.GCTimestampNext; t.IsEmpty() || gcThreshold.Less(t) {
		h.GCTimestamp, h.GCTimestampNext = h.GCTimestampNext, hlc.Timestamp{}
		return true
	}
	// If threshold >= max, erase both min and max.
	h.GCTimestamp, h.GCTimestampNext = hlc.Timestamp{}, hlc.Timestamp{}
	return true
}

type RangeDescriptorsByStartKey []RangeDescriptor

func (r RangeDescriptorsByStartKey) Len() int {
	return len(r)
}
func (r RangeDescriptorsByStartKey) Less(i, j int) bool {
	return r[i].StartKey.AsRawKey().Less(r[j].StartKey.AsRawKey())
}

func (r RangeDescriptorsByStartKey) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
