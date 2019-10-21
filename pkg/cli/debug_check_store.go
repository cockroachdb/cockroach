// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/raft/raftpb"
)

var debugCheckStoreCmd = &cobra.Command{
	Use:   "check-store <directory>",
	Short: "consistency check for a single store",
	Long: `
Perform local consistency checks of a single store.

Capable of detecting the following errors:
* Raft logs that are inconsistent with their metadata
* MVCC stats that are inconsistent with the data within the range
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugCheckStoreCmd),
}

type replicaCheckInfo struct {
	truncatedIndex uint64
	appliedIndex   uint64
	firstIndex     uint64
	lastIndex      uint64
	committedIndex uint64
}

func runDebugCheckStoreCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}
	var hasError bool
	if err := runDebugCheckStoreRaft(ctx, db); err != nil {
		hasError = true
		log.Warning(ctx, err)
	}
	if err := runDebugCheckStoreDescriptors(ctx, db); err != nil {
		hasError = true
		log.Warning(ctx, err)
	}
	if hasError {
		return errors.New("errors detected")
	}
	return nil
}

func runDebugCheckStoreDescriptors(ctx context.Context, db *engine.RocksDB) error {
	fmt.Println("checking MVCC stats")
	defer fmt.Println()

	var failed bool
	if err := storage.IterateRangeDescriptors(ctx, db,
		func(desc roachpb.RangeDescriptor) (bool, error) {
			claimedMS, err := stateloader.Make(desc.RangeID).LoadMVCCStats(ctx, db)
			if err != nil {
				return false, err
			}
			ms, err := rditer.ComputeStatsForRange(&desc, db, claimedMS.LastUpdateNanos)
			if err != nil {
				return false, err
			}

			if !ms.Equal(claimedMS) {
				var prefix string
				if !claimedMS.ContainsEstimates {
					failed = true
				} else {
					ms.ContainsEstimates = true
					prefix = "(ignored) "
				}
				fmt.Printf("\n%s%+v: diff(actual, claimed): %s\n", prefix, desc, strings.Join(pretty.Diff(ms, claimedMS), "\n"))
			}
			return false, nil
		}); err != nil {
		return err
	}
	if failed {
		return errors.New("check failed")
	}
	return nil
}

func runDebugCheckStoreRaft(ctx context.Context, db *engine.RocksDB) error {
	// Iterate over the entire range-id-local space.
	start := roachpb.Key(keys.LocalRangeIDPrefix)
	end := start.PrefixEnd()

	replicaInfo := map[roachpb.RangeID]*replicaCheckInfo{}
	getReplicaInfo := func(rangeID roachpb.RangeID) *replicaCheckInfo {
		if info, ok := replicaInfo[rangeID]; ok {
			return info
		}
		replicaInfo[rangeID] = &replicaCheckInfo{}
		return replicaInfo[rangeID]
	}

	var hasError bool

	if _, err := engine.MVCCIterate(ctx, db, start, end, hlc.MaxTimestamp,
		engine.MVCCScanOptions{Inconsistent: true}, func(kv roachpb.KeyValue) (bool, error) {
			rangeID, _, suffix, detail, err := keys.DecodeRangeIDKey(kv.Key)
			if err != nil {
				return false, err
			}

			switch {
			case bytes.Equal(suffix, keys.LocalRaftHardStateSuffix):
				var hs raftpb.HardState
				if err := kv.Value.GetProto(&hs); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).committedIndex = hs.Commit
			case bytes.Equal(suffix, keys.LocalRaftTruncatedStateLegacySuffix):
				var trunc roachpb.RaftTruncatedState
				if err := kv.Value.GetProto(&trunc); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).truncatedIndex = trunc.Index
			case bytes.Equal(suffix, keys.LocalRangeAppliedStateSuffix):
				var state enginepb.RangeAppliedState
				if err := kv.Value.GetProto(&state); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).appliedIndex = state.RaftAppliedIndex
			case bytes.Equal(suffix, keys.LocalRaftAppliedIndexLegacySuffix):
				idx, err := kv.Value.GetInt()
				if err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).appliedIndex = uint64(idx)
			case bytes.Equal(suffix, keys.LocalRaftLogSuffix):
				_, index, err := encoding.DecodeUint64Ascending(detail)
				if err != nil {
					return false, err
				}
				ri := getReplicaInfo(rangeID)
				if ri.firstIndex == 0 {
					ri.firstIndex = index
					ri.lastIndex = index
				} else {
					if index != ri.lastIndex+1 {
						fmt.Printf("range %s: log index anomaly: %v followed by %v\n",
							rangeID, ri.lastIndex, index)
						hasError = true
					}
					ri.lastIndex = index
				}
			}

			return false, nil
		}); err != nil {
		return err
	}

	for rangeID, info := range replicaInfo {
		if info.truncatedIndex != 0 && info.truncatedIndex != info.firstIndex-1 {
			hasError = true
			fmt.Printf("range %s: truncated index %v should equal first index %v - 1\n",
				rangeID, info.truncatedIndex, info.firstIndex)
		}
		if info.firstIndex > info.lastIndex {
			hasError = true
			fmt.Printf("range %s: [first index, last index] is [%d, %d]\n",
				rangeID, info.firstIndex, info.lastIndex)
		}
		if info.appliedIndex < info.firstIndex || info.appliedIndex > info.lastIndex {
			hasError = true
			fmt.Printf("range %s: applied index %v should be between first index %v and last index %v\n",
				rangeID, info.appliedIndex, info.firstIndex, info.lastIndex)
		}
		if info.appliedIndex > info.committedIndex {
			hasError = true
			fmt.Printf("range %s: committed index %d must not trail applied index %d\n",
				rangeID, info.committedIndex, info.appliedIndex)
		}
		if info.committedIndex > info.lastIndex {
			hasError = true
			fmt.Printf("range %s: committed index %d ahead of last index  %d\n",
				rangeID, info.committedIndex, info.lastIndex)
		}
	}
	if hasError {
		return errors.New("anomalies detected in Raft state")
	}

	return nil
}
