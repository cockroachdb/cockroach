// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
)

func loadEventRequestCPU(c config.ResourceCost, le workload.LoadEvent) float64 {
	read := int64(math.Min(float64(le.Reads), 1))
	write := int64(math.Min(float64(le.Writes), 1))

	batchCost := read*c.ReadBatch + read*c.Read + write*c.WriteBatch
	return float64(c.Read*le.Reads + c.ReadByte*le.Reads + batchCost)
}

func loadEventRaftCPU(c config.ResourceCost, le workload.LoadEvent) float64 {
	write := int64(math.Min(float64(le.Writes), 1))
	return float64(c.WriteByte*le.WriteSize + c.Write*write)
}
