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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/workload"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
)

func loadEventCPU(c tenantcostmodel.Config, le workload.LoadEvent, leaseholder bool) float64 {
	var readCPU, writeCPU float64
	if leaseholder {
		readCPU = float64(c.KVReadCost(le.Reads, le.ReadSize))
	}
	writeCPU = float64(c.KVWriteCost(le.Writes, le.WriteSize))

	return readCPU + writeCPU
}
