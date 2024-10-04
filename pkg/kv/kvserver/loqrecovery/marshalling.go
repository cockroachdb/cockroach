// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// MarshalReplicaInfo serializes info into version dependent format. This is
// needed to create replica info files in format compatible with analyzed
// cluster version. For example if cluster is partially upgraded and node with
// new binary is collecting offline info for processing it should be created
// in a format compatible with node that will produce a plan which must create
// a plan compatible with offline application and could be either old or new
// binary.
func MarshalReplicaInfo(replicaInfo loqrecoverypb.ClusterReplicaInfo) ([]byte, error) {
	jsonpb := protoutil.JSONPb{Indent: "  "}

	v := clusterversion.ClusterVersion{
		Version: replicaInfo.Version,
	}
	if v.IsActive(clusterversion.V23_1) {
		out, err := jsonpb.Marshal(&replicaInfo)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal replica info")
		}
		return out, nil
	}

	var combined []loqrecoverypb.ReplicaInfo
	for _, i := range replicaInfo.LocalInfo {
		combined = append(combined, i.Replicas...)
	}
	// NB: this marshalling is incorrect, but we preserve a bug for backward
	// compatibility. Message pointer is implementing interface, not struct
	// itself. See Marshal below on how it must be done.
	out, err := jsonpb.Marshal(loqrecoverypb.NodeReplicaInfo{Replicas: combined})
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal replica info in legacy (22.1) format")
	}
	return out, nil
}

// UnmarshalReplicaInfo tries to guess format and deserialize data. Old format
// expects only a single field, so we try that first, if it fails, fall back to
// current format.
func UnmarshalReplicaInfo(data []byte) (loqrecoverypb.ClusterReplicaInfo, error) {
	jsonpb := protoutil.JSONPb{}
	var nodeReplicas loqrecoverypb.NodeReplicaInfo
	if err := jsonpb.Unmarshal(data, &nodeReplicas); err == nil {
		return loqrecoverypb.ClusterReplicaInfo{
			LocalInfo: []loqrecoverypb.NodeReplicaInfo{nodeReplicas},
			Version:   legacyInfoFormatVersion,
		}, nil
	}

	var clusterReplicas loqrecoverypb.ClusterReplicaInfo
	if err := jsonpb.Unmarshal(data, &clusterReplicas); err != nil {
		return loqrecoverypb.ClusterReplicaInfo{}, err
	}
	if err := checkVersionAllowedByBinary(clusterReplicas.Version); err != nil {
		return loqrecoverypb.ClusterReplicaInfo{}, errors.Wrap(err, "unsupported cluster info version")
	}
	return clusterReplicas, nil
}

// legacyPlan mimics serialization of ReplicaUpdatePlan when using value instead
// of pointer and excludes all new fields that would be serialized as empty
// values otherwise.
type legacyPlan struct {
	Updates []loqrecoverypb.ReplicaUpdate `json:"updates"`
}

// MarshalPlan writes replica update plan in format compatible with target
// version.
func MarshalPlan(plan loqrecoverypb.ReplicaUpdatePlan) ([]byte, error) {
	jsonpb := protoutil.JSONPb{Indent: "  "}

	v := clusterversion.ClusterVersion{
		Version: plan.Version,
	}
	if v.IsActive(clusterversion.V23_1) {
		out, err := jsonpb.Marshal(&plan)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal recovery plan")
		}
		return out, nil
	}
	out, err := jsonpb.Marshal(legacyPlan{Updates: plan.Updates})
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal recovery plan in v1 format")
	}
	return out, nil
}

// UnmarshalPlan reads json containing replica update plan into struct. There's
// no special version handling for reading plans as formats are compatible.
func UnmarshalPlan(data []byte) (loqrecoverypb.ReplicaUpdatePlan, error) {
	var nodeUpdates loqrecoverypb.ReplicaUpdatePlan
	jsonpb := protoutil.JSONPb{Indent: "  "}
	if err := jsonpb.Unmarshal(data, &nodeUpdates); err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, err
	}
	return nodeUpdates, nil
}
