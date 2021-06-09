// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// IngestBatchSize is a cluster setting that controls the maximum size of the
// payload in an AddSSTable request.
var IngestBatchSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		"kv.bulk_ingest.batch_size",
		"the maximum size of the payload in an AddSSTable request",
		16<<20,
	)
	return s
}()

// commandMetadataEstimate is an estimate of how much metadata Raft will add to
// an AddSSTable command. It is intentionally a vast overestimate to avoid
// embedding intricate knowledge of the Raft encoding scheme here.
const commandMetadataEstimate = 1 << 20 // 1 MB

func init() {
	// Ensure that the user cannot set the maximum raft command size so low that
	// more than half of an Import or AddSSTable command will be taken up by Raft
	// metadata.
	if commandMetadataEstimate > kvserver.MaxCommandSizeFloor/2 {
		panic(fmt.Sprintf("raft command size floor (%s) is too small for import commands",
			humanizeutil.IBytes(kvserver.MaxCommandSizeFloor)))
	}
}

// MaxIngestBatchSize determines the maximum size of the payload in an
// AddSSTable request. It uses the IngestBatchSize setting directly unless the
// specified value would exceed the maximum Raft command size, in which case it
// returns the maximum batch size that will fit within a Raft command.
func MaxIngestBatchSize(st *cluster.Settings) int64 {
	desiredSize := IngestBatchSize.Get(&st.SV)
	maxCommandSize := kvserver.MaxCommandSize.Get(&st.SV)
	if desiredSize+commandMetadataEstimate > maxCommandSize {
		return maxCommandSize - commandMetadataEstimate
	}
	return desiredSize
}
