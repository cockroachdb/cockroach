// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
)

// NodeID is a custom type for a cockroach node ID. (not a raft node ID)
// 0 is not a valid NodeID.
type NodeID int32

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n NodeID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// StoreID is a custom type for a cockroach store ID.
type StoreID int32

// StoreIDSlice implements sort.Interface.
type StoreIDSlice []StoreID

func (s StoreIDSlice) Len() int           { return len(s) }
func (s StoreIDSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StoreIDSlice) Less(i, j int) bool { return s[i] < s[j] }

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n StoreID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// A RangeID is a unique ID associated to a Raft consensus group.
type RangeID int64

// String implements the fmt.Stringer interface.
func (r RangeID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

// RangeIDSlice implements sort.Interface.
type RangeIDSlice []RangeID

func (r RangeIDSlice) Len() int           { return len(r) }
func (r RangeIDSlice) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r RangeIDSlice) Less(i, j int) bool { return r[i] < r[j] }

// ReplicaID is a custom type for a range replica ID.
type ReplicaID int32

// String implements the fmt.Stringer interface.
func (r ReplicaID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

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

// NewRangeDescriptor returns a RangeDescriptor populated from the input.
func NewRangeDescriptor(
	rangeID RangeID, start, end RKey, replicas ReplicaDescriptors,
) *RangeDescriptor {
	repls := append([]ReplicaDescriptor(nil), replicas.All()...)
	for i := range repls {
		repls[i].ReplicaID = ReplicaID(i + 1)
	}
	desc := &RangeDescriptor{
		RangeID:       rangeID,
		StartKey:      start,
		EndKey:        end,
		NextReplicaID: ReplicaID(len(repls) + 1),
	}
	desc.SetReplicas(MakeReplicaDescriptors(repls))
	return desc
}

// RSpan returns the RangeDescriptor's resolved span.
func (r *RangeDescriptor) RSpan() RSpan {
	return RSpan{Key: r.StartKey, EndKey: r.EndKey}
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
func (r *RangeDescriptor) Replicas() ReplicaDescriptors {
	return MakeReplicaDescriptors(r.InternalReplicas)
}

// SetReplicas overwrites the set of nodes/stores on which replicas of this
// range are stored.
func (r *RangeDescriptor) SetReplicas(replicas ReplicaDescriptors) {
	r.InternalReplicas = replicas.AsProto()
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
			prevTyp := desc.GetType()
			if typ != VOTER_FULL {
				desc.Type = &typ
			} else {
				// For 19.1 compatibility.
				desc.Type = nil
			}
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
	var typPtr *ReplicaType
	// For 19.1 compatibility, use nil instead of VOTER_FULL.
	if typ != VOTER_FULL {
		typPtr = &typ
	}
	toAdd := ReplicaDescriptor{
		NodeID:    nodeID,
		StoreID:   storeID,
		ReplicaID: r.NextReplicaID,
		Type:      typPtr,
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
	for _, repDesc := range r.Replicas().All() {
		if repDesc.StoreID == storeID {
			return repDesc, true
		}
	}
	return ReplicaDescriptor{}, false
}

// GetReplicaDescriptorByID returns the replica which matches the specified store
// ID.
func (r *RangeDescriptor) GetReplicaDescriptorByID(replicaID ReplicaID) (ReplicaDescriptor, bool) {
	for _, repDesc := range r.Replicas().All() {
		if repDesc.ReplicaID == replicaID {
			return repDesc, true
		}
	}
	return ReplicaDescriptor{}, false
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

// GetStickyBit returns the sticky bit of this RangeDescriptor.
func (r *RangeDescriptor) GetStickyBit() hlc.Timestamp {
	if r.StickyBit == nil {
		return hlc.Timestamp{}
	}
	return *r.StickyBit
}

// Validate performs some basic validation of the contents of a range descriptor.
func (r *RangeDescriptor) Validate() error {
	if r.NextReplicaID == 0 {
		return errors.Errorf("NextReplicaID must be non-zero")
	}
	seen := map[ReplicaID]struct{}{}
	stores := map[StoreID]struct{}{}
	for i, rep := range r.Replicas().All() {
		if err := rep.Validate(); err != nil {
			return errors.Errorf("replica %d is invalid: %s", i, err)
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
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "r%d:", r.RangeID)

	if !r.IsInitialized() {
		buf.WriteString("{-}")
	} else {
		buf.WriteString(r.RSpan().String())
	}
	buf.WriteString(" [")

	if allReplicas := r.Replicas().All(); len(allReplicas) > 0 {
		for i, rep := range allReplicas {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(rep.String())
		}
	} else {
		buf.WriteString("<no replicas>")
	}
	fmt.Fprintf(&buf, ", next=%d, gen=%d", r.NextReplicaID, r.Generation)
	if s := r.GetStickyBit(); !s.IsEmpty() {
		fmt.Fprintf(&buf, ", sticky=%s", s)
	}
	buf.WriteString("]")

	return buf.String()
}

func (r ReplicationTarget) String() string {
	return fmt.Sprintf("n%d,s%d", r.NodeID, r.StoreID)
}

func (r ReplicaDescriptor) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "(n%d,s%d):", r.NodeID, r.StoreID)
	if r.ReplicaID == 0 {
		buf.WriteString("?")
	} else {
		fmt.Fprintf(&buf, "%d", r.ReplicaID)
	}
	if typ := r.GetType(); typ != VOTER_FULL {
		buf.WriteString(typ.String())
	}
	return buf.String()
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

// GetType returns the type of this ReplicaDescriptor.
func (r ReplicaDescriptor) GetType() ReplicaType {
	if r.Type == nil {
		return VOTER_FULL
	}
	return *r.Type
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
	return fmt.Sprintf("p10=%.2f p25=%.2f p50=%.2f p75=%.2f p90=%.2f pMax=%.2f",
		p.P10, p.P25, p.P50, p.P75, p.P90, p.PMax)
}

// String returns a string representation of the StoreCapacity.
func (sc StoreCapacity) String() string {
	return fmt.Sprintf("disk (capacity=%s, available=%s, used=%s, logicalBytes=%s), "+
		"ranges=%d, leases=%d, queries=%.2f, writes=%.2f, "+
		"bytesPerReplica={%s}, writesPerReplica={%s}",
		humanizeutil.IBytes(sc.Capacity), humanizeutil.IBytes(sc.Available),
		humanizeutil.IBytes(sc.Used), humanizeutil.IBytes(sc.LogicalBytes),
		sc.RangeCount, sc.LeaseCount, sc.QueriesPerSecond, sc.WritesPerSecond,
		sc.BytesPerReplica, sc.WritesPerReplica)
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
	// Fall back to a more pessimistic calcuation of disk usage if we don't know
	// how much space the store's data is taking up.
	if sc.Used == 0 {
		return float64(sc.Capacity-sc.Available) / float64(sc.Capacity)
	}
	return float64(sc.Used) / float64(sc.Available+sc.Used)
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
