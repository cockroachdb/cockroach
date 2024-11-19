// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
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
	out, err := jsonpb.Marshal(&replicaInfo)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal replica info")
	}
	return out, nil
}

// UnmarshalReplicaInfo deserializes ClusterReplicaInfo.
func UnmarshalReplicaInfo(data []byte) (loqrecoverypb.ClusterReplicaInfo, error) {
	jsonpb := protoutil.JSONPb{}
	var clusterReplicas loqrecoverypb.ClusterReplicaInfo
	if err := jsonpb.Unmarshal(data, &clusterReplicas); err != nil {
		return loqrecoverypb.ClusterReplicaInfo{}, err
	}
	if err := checkVersionAllowedByBinary(clusterReplicas.Version); err != nil {
		return loqrecoverypb.ClusterReplicaInfo{}, errors.Wrap(err, "unsupported cluster info version")
	}
	return clusterReplicas, nil
}

// MarshalPlan writes replica update plan in format compatible with target
// version.
func MarshalPlan(plan loqrecoverypb.ReplicaUpdatePlan) ([]byte, error) {
	jsonpb := protoutil.JSONPb{Indent: "  "}

	out, err := jsonpb.Marshal(&plan)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal recovery plan")
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
