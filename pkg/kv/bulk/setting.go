// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

var (
	// IngestBatchSize controls the size of ingest ssts.
	IngestBatchSize = settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"kv.bulk_ingest.batch_size",
		"the maximum size of the payload in an AddSSTable request",
		16<<20,
	)
)

// ingestFileSize determines the target size files sent via AddSSTable requests.
// It returns the smaller of the IngestBatchSize and Raft command size settings.
func ingestFileSize(st *cluster.Settings) int64 {
	desiredSize := IngestBatchSize.Get(&st.SV)
	maxCommandSize := kvserverbase.MaxCommandSize.Get(&st.SV)
	if desiredSize > maxCommandSize {
		return maxCommandSize
	}
	return desiredSize
}
