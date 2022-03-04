// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

var (
	// IngestBatchSize controls the size of ingest ssts.
	IngestBatchSize = settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"kv.bulk_ingest.batch_size",
		"the maximum size of the payload in an AddSSTable request",
		16<<20,
	)
)

// ingestFileSize determines the target size files sent via AddSSTable requests.
// It returns the smaller of the IngestBatchSize and Raft command size settings.
func ingestFileSize(st *cluster.Settings) int64 {
	desiredSize := IngestBatchSize.Get(&st.SV)
	maxCommandSize := kvserver.MaxCommandSize.Get(&st.SV)
	if desiredSize > maxCommandSize {
		return maxCommandSize
	}
	return desiredSize
}
