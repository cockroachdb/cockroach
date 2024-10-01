// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecoverypb

import (
	_ "github.com/cockroachdb/cockroach/pkg/kv/kvpb" // Needed for recovery.proto.
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	_ "github.com/cockroachdb/cockroach/pkg/util/uuid" // needed for recovery.proto
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// RecoveryKey is an alias for RKey that is used to make it
// yaml serializable. Caution must be taken to use produced
// representation outside of tests.
type RecoveryKey roachpb.RKey

// MarshalYAML implements Marshaler interface.
func (r RecoveryKey) MarshalYAML() (interface{}, error) {
	return roachpb.RKey(r).String(), nil
}

// UnmarshalYAML implements Unmarshaler interface.
func (r *RecoveryKey) UnmarshalYAML(fn func(interface{}) error) error {
	var pretty string
	if err := fn(&pretty); err != nil {
		return err
	}
	scanner := keysutil.MakePrettyScanner(nil /* tableParser */, nil /* tenantParser */)
	key, err := scanner.Scan(pretty)
	if err != nil {
		return errors.Wrapf(err, "failed to parse key %s", pretty)
	}
	*r = RecoveryKey(key)
	return nil
}

// AsRKey returns key as a cast to RKey.
func (r RecoveryKey) AsRKey() roachpb.RKey {
	return roachpb.RKey(r)
}

func (m ReplicaUpdate) String() string {
	return proto.CompactTextString(&m)
}

// NodeID is a NodeID on which this replica update should be applied.
func (m ReplicaUpdate) NodeID() roachpb.NodeID {
	return m.NewReplica.NodeID
}

// StoreID is a StoreID on which this replica update should be applied.
func (m ReplicaUpdate) StoreID() roachpb.StoreID {
	return m.NewReplica.StoreID
}

// Replica gets replica for the store where this info and range
// descriptor were collected. Returns err if it can't find replica
// descriptor for the store it originated from.
func (m *ReplicaInfo) Replica() (roachpb.ReplicaDescriptor, error) {
	if d, ok := m.Desc.GetReplicaDescriptor(m.StoreID); ok {
		return d, nil
	}
	return roachpb.ReplicaDescriptor{}, errors.Errorf(
		"invalid replica info: its own store s%d is not present in descriptor replicas %s",
		m.StoreID, m.Desc)
}

// AsStructuredLog creates a structured log entry from the record.
func (m *ReplicaRecoveryRecord) AsStructuredLog() eventpb.DebugRecoverReplica {
	return eventpb.DebugRecoverReplica{
		CommonEventDetails: logpb.CommonEventDetails{
			Timestamp: m.Timestamp,
		},
		CommonDebugEventDetails: eventpb.CommonDebugEventDetails{
			NodeID: int32(m.NewReplica.NodeID),
		},
		RangeID:           int64(m.RangeID),
		StoreID:           int64(m.NewReplica.StoreID),
		SurvivorReplicaID: int32(m.OldReplicaID),
		UpdatedReplicaID:  int32(m.NewReplica.ReplicaID),
		StartKey:          m.StartKey.AsRKey().String(),
		EndKey:            m.EndKey.AsRKey().String(),
	}
}

func (m *ClusterReplicaInfo) Merge(o ClusterReplicaInfo) error {
	// When making a cluster id check, make sure that we can create empty
	// cluster info and merge everything into it. i.e. merging into empty
	// struct should not trip check failure.
	if len(m.LocalInfo) > 0 || len(m.Descriptors) > 0 {
		if m.ClusterID != o.ClusterID {
			return errors.Newf("can't merge cluster info from different cluster: %s != %s", m.ClusterID,
				o.ClusterID)
		}
		if !m.Version.Equal(o.Version) {
			return errors.Newf("can't merge cluster info from different version: %s != %s", m.Version,
				o.Version)
		}
	} else {
		m.ClusterID = o.ClusterID
		m.Version = o.Version
	}
	if len(o.Descriptors) > 0 {
		if len(m.Descriptors) > 0 {
			return errors.New("only single cluster replica info could contain descriptors")
		}
		m.Descriptors = append(m.Descriptors, o.Descriptors...)
	}
	type nsk struct {
		n roachpb.NodeID
		s roachpb.StoreID
	}
	existing := make(map[nsk]struct{})
	for _, n := range m.LocalInfo {
		for _, r := range n.Replicas {
			existing[nsk{n: r.NodeID, s: r.StoreID}] = struct{}{}
		}
	}
	for _, n := range o.LocalInfo {
		for _, r := range n.Replicas {
			if _, ok := existing[nsk{n: r.NodeID, s: r.StoreID}]; ok {
				return errors.Newf("failed to merge cluster info, replicas from n%d/s%d are already present",
					r.NodeID, r.StoreID)
			}
		}
	}
	m.LocalInfo = append(m.LocalInfo, o.LocalInfo...)
	return nil
}

func (m *ClusterReplicaInfo) ReplicaCount() (size int) {
	for _, i := range m.LocalInfo {
		size += len(i.Replicas)
	}
	return size
}

func (a DeferredRecoveryActions) Empty() bool {
	return len(a.DecommissionedNodeIDs) == 0
}

var rangeHealthTitle = map[int32]string{
	0: "unknown",
	1: "healthy",
	2: "waiting for meta",
	3: "loss of quorum",
}

// Name gives a better looking name for range health enum which is good for
// including in CLI messages.
func (x RangeHealth) Name() string {
	return proto.EnumName(rangeHealthTitle, int32(x))
}
