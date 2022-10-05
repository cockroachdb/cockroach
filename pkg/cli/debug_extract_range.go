// Copyright 2022 The Cockroach Authors.
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
	"context"
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/spf13/cobra"
)

var debugExtractRangeCmd = &cobra.Command{
	Use:   "extract-range <directory> <range-ID> <to-file>",
	Short: "extract range data from a store or checkpoint",
	Long: `
Reads the content of a range from the store or a checkpoint, and saves it to the
given file.
`,
	Args: cobra.ExactArgs(3),
	RunE: clierrorplus.MaybeDecorateError(runDebugExtractRangeCmd),
}

func runDebugExtractRangeCmd(_ *cobra.Command, args []string) error {
	// Parse parameters.
	dir, filename := args[0], args[2]
	rID, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return err
	}
	rangeID := roachpb.RangeID(rID)

	// Open storage for reading.
	ctx := context.Background()
	st, err := storage.Open(ctx, storage.Filesystem(dir), storage.ReadOnly, storage.MustExist)
	if err != nil {
		return err
	}
	defer st.Close()

	// Find the range in the storage.
	var desc *roachpb.RangeDescriptor
	err = kvserver.IterateRangeDescriptorsFromDisk(ctx, st, func(rd roachpb.RangeDescriptor) error {
		if rd.RangeID == rangeID {
			desc = &rd
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Load the content of the range.
	snap, err := kvserver.LoadRaftSnapshotDataForTesting(ctx, *desc, st)
	if err != nil {
		return err
	}

	// Write the range content to the file.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer f.Close()
	// TODO(pavelkalinnikov): Export as a binary SST, push formatting options to
	// the SST printing/diffing tool.
	if _, err := f.WriteString(formatSnapshotData(&snap)); err != nil {
		return err
	}
	return nil
}

func formatSnapshotData(data *roachpb.RaftSnapshotData) string {
	var entries kvserver.ReplicaSnapshotDiffSlice
	for _, e := range data.KV {
		entries = append(entries, kvserver.ReplicaSnapshotDiff{
			Key:       e.Key,
			Timestamp: e.Timestamp,
			Value:     e.Value,
		})
	}
	for _, e := range data.RangeKV {
		entries = append(entries, kvserver.ReplicaSnapshotDiff{
			Key:       e.StartKey,
			EndKey:    e.EndKey,
			Timestamp: e.Timestamp,
			Value:     e.Value,
		})
	}
	return entries.String()
}
