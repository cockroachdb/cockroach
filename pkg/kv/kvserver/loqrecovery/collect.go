// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type CollectionStats struct {
	Nodes       int
	Stores      int
	Descriptors int
}

// CollectRemoteReplicaInfo retrieves information about:
//  1. range descriptors contained in cluster meta ranges if meta ranges
//     are readable;
//  2. replica information from all live nodes that have connection to
//     the target node.
//
// maxConcurrency is the maximum parallelism that will be used when fanning out
// RPCs to nodes in the cluster. A value of 0 disables concurrency. A negative
// value configures no limit for concurrency.
// If logOutput is not nil, this function will write when a node is visited,
// and when a node needs to be revisited.
func CollectRemoteReplicaInfo(
	ctx context.Context, c serverpb.AdminClient, maxConcurrency int, logOutput io.Writer,
) (loqrecoverypb.ClusterReplicaInfo, CollectionStats, error) {
	cc, err := c.RecoveryCollectReplicaInfo(ctx, &serverpb.RecoveryCollectReplicaInfoRequest{
		MaxConcurrency: int32(maxConcurrency),
	})
	if err != nil {
		return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, err
	}
	stores := make(map[roachpb.StoreID]struct{})
	nodes := make(map[roachpb.NodeID]struct{})
	replInfoMap := make(map[roachpb.NodeID][]loqrecoverypb.ReplicaInfo)
	var descriptors []roachpb.RangeDescriptor
	var metadata loqrecoverypb.ClusterMetadata
	for {
		info, err := cc.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, err
		}
		if r := info.GetReplicaInfo(); r != nil {
			stores[r.StoreID] = struct{}{}
			nodes[r.NodeID] = struct{}{}

			if _, ok := replInfoMap[r.NodeID]; !ok && logOutput != nil {
				_, _ = fmt.Fprintf(logOutput, "Started getting replica info for node_id:%d.\n", r.NodeID)
			}
			replInfoMap[r.NodeID] = append(replInfoMap[r.NodeID], *r)
		} else if d := info.GetRangeDescriptor(); d != nil {
			descriptors = append(descriptors, *d)
		} else if s := info.GetNodeStreamRestarted(); s != nil {
			// If server had to restart a fan-out work because of error and retried,
			// then we discard partial data for the node.
			delete(replInfoMap, s.NodeID)
			if logOutput != nil {
				_, _ = fmt.Fprintf(logOutput, "Discarding replica info for node_id:%d."+
					"The node will be revisted.\n", s.NodeID)
			}

		} else if m := info.GetMetadata(); m != nil {
			metadata = *m
		} else {
			return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, errors.AssertionFailedf(
				"unknown info type: %T", info.GetInfo())
		}
	}
	// Collapse the ReplicaInfos map into a slice, sorted by node ID.
	replInfos := make([]loqrecoverypb.NodeReplicaInfo, 0, len(replInfoMap))
	for _, replInfo := range replInfoMap {
		if len(replInfo) == 0 {
			continue
		}
		replInfos = append(replInfos, loqrecoverypb.NodeReplicaInfo{Replicas: replInfo})
	}
	slices.SortFunc(replInfos, func(a, b loqrecoverypb.NodeReplicaInfo) int {
		return cmp.Compare(a.Replicas[0].NodeID, b.Replicas[0].NodeID)
	})
	// We don't want to process data outside of safe version range for this CLI
	// binary. RPC allows us to communicate with a cluster that is newer than
	// the binary, but it will not version gate the data to binary version so we
	// can receive entries that we won't be able to persist and process correctly.
	if err := checkVersionAllowedByBinary(metadata.Version); err != nil {
		return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, errors.Wrap(err,
			"unsupported cluster info version")
	}
	return loqrecoverypb.ClusterReplicaInfo{
			ClusterID:   metadata.ClusterID,
			Descriptors: descriptors,
			LocalInfo:   replInfos,
			Version:     metadata.Version,
		}, CollectionStats{
			Nodes:       len(nodes),
			Stores:      len(stores),
			Descriptors: len(descriptors),
		}, nil
}

// CollectStoresReplicaInfo captures states of all replicas in all stores for the sake of quorum recovery.
func CollectStoresReplicaInfo(
	ctx context.Context, stores []storage.Engine,
) (loqrecoverypb.ClusterReplicaInfo, CollectionStats, error) {
	if len(stores) == 0 {
		return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, errors.New("no stores were provided for info collection")
	}

	// Synthesizing version from engine ensures that binary is compatible with
	// the store, so we don't need to do any extra checks.
	binaryVersion := clusterversion.Latest.Version()
	binaryMinSupportedVersion := clusterversion.MinSupported.Version()
	version, err := kvstorage.SynthesizeClusterVersionFromEngines(
		ctx, stores, binaryVersion, binaryMinSupportedVersion,
	)
	if err != nil {
		return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, errors.WithHint(err,
			"ensure that used cli has compatible version with storage")
	}

	var clusterUUID uuid.UUID
	nodes := make(map[roachpb.NodeID]struct{})
	var replicas []loqrecoverypb.ReplicaInfo
	for i, reader := range stores {
		ident, err := kvstorage.ReadStoreIdent(ctx, reader)
		if err != nil {
			return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, err
		}
		if i == 0 {
			clusterUUID = ident.ClusterID
		}
		if !ident.ClusterID.Equal(clusterUUID) {
			return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, errors.New("can't collect info from stored that belong to different clusters")
		}
		nodes[ident.NodeID] = struct{}{}
		if err := visitStoreReplicas(ctx, reader, ident.StoreID, ident.NodeID, version,
			func(info loqrecoverypb.ReplicaInfo) error {
				replicas = append(replicas, info)
				return nil
			}); err != nil {
			return loqrecoverypb.ClusterReplicaInfo{}, CollectionStats{}, err
		}
	}
	return loqrecoverypb.ClusterReplicaInfo{
			ClusterID: clusterUUID.String(),
			LocalInfo: []loqrecoverypb.NodeReplicaInfo{{Replicas: replicas}},
			Version:   version.Version,
		}, CollectionStats{
			Nodes:  len(nodes),
			Stores: len(stores),
		}, nil
}

func visitStoreReplicas(
	ctx context.Context,
	reader storage.Reader,
	storeID roachpb.StoreID,
	nodeID roachpb.NodeID,
	targetVersion clusterversion.ClusterVersion,
	send func(info loqrecoverypb.ReplicaInfo) error,
) error {
	if err := kvstorage.IterateRangeDescriptorsFromDisk(ctx, reader, func(desc roachpb.RangeDescriptor) error {
		rsl := stateloader.Make(desc.RangeID)
		rstate, err := rsl.Load(ctx, reader, &desc)
		if err != nil {
			return err
		}
		hstate, err := rsl.LoadHardState(ctx, reader)
		if err != nil {
			return err
		}
		// Check raft log for un-applied range descriptor changes. We start from
		// applied+1 (inclusive) and read until the end of the log. We also look
		// at potentially uncommitted entries as we have no way to determine their
		// outcome, and they will become committed as soon as the replica is
		// designated as a survivor.
		rangeUpdates, err := GetDescriptorChangesFromRaftLog(
			ctx, desc.RangeID, rstate.RaftAppliedIndex+1, math.MaxInt64, reader)
		if err != nil {
			return err
		}

		localIsLeaseholder := rstate.Lease != nil && rstate.Lease.Replica.StoreID == storeID

		return send(loqrecoverypb.ReplicaInfo{
			StoreID:                  storeID,
			NodeID:                   nodeID,
			Desc:                     desc,
			RaftAppliedIndex:         rstate.RaftAppliedIndex,
			RaftCommittedIndex:       kvpb.RaftIndex(hstate.Commit),
			RaftLogDescriptorChanges: rangeUpdates,
			LocalAssumesLeaseholder:  localIsLeaseholder,
		})
	}); err != nil {
		return err
	}
	return nil
}

// GetDescriptorChangesFromRaftLog iterates over raft log between indices
// lo (inclusive) and hi (exclusive) and searches for changes to range
// descriptors, as identified by presence of a commit trigger.
func GetDescriptorChangesFromRaftLog(
	ctx context.Context, rangeID roachpb.RangeID, lo, hi kvpb.RaftIndex, reader storage.Reader,
) ([]loqrecoverypb.DescriptorChangeInfo, error) {
	var changes []loqrecoverypb.DescriptorChangeInfo
	if err := raftlog.Visit(ctx, reader, rangeID, lo, hi, func(ent raftpb.Entry) error {
		e, err := raftlog.NewEntry(ent)
		if err != nil {
			return err
		}
		raftCmd := e.Cmd
		switch {
		case raftCmd.ReplicatedEvalResult.Split != nil:
			changes = append(changes,
				loqrecoverypb.DescriptorChangeInfo{
					ChangeType: loqrecoverypb.DescriptorChangeType_Split,
					Desc:       &raftCmd.ReplicatedEvalResult.Split.LeftDesc,
					OtherDesc:  &raftCmd.ReplicatedEvalResult.Split.RightDesc,
				})
		case raftCmd.ReplicatedEvalResult.Merge != nil:
			changes = append(changes,
				loqrecoverypb.DescriptorChangeInfo{
					ChangeType: loqrecoverypb.DescriptorChangeType_Merge,
					Desc:       &raftCmd.ReplicatedEvalResult.Merge.LeftDesc,
					OtherDesc:  &raftCmd.ReplicatedEvalResult.Merge.RightDesc,
				})
		case raftCmd.ReplicatedEvalResult.ChangeReplicas != nil:
			changes = append(changes, loqrecoverypb.DescriptorChangeInfo{
				ChangeType: loqrecoverypb.DescriptorChangeType_ReplicaChange,
				Desc:       raftCmd.ReplicatedEvalResult.ChangeReplicas.Desc,
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return changes, nil
}
