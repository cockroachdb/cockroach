// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package ddl_analysisccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	// Blank import partitionccl to install CreatePartitioning hook.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/sql"
)

func init() {
	sql.InitializeMultiRegionMetadataCCL = multiregionccl.InitializeMultiRegionMetadata
	sql.GetMultiRegionEnumAddValuePlacementCCL = multiregionccl.GetMultiRegionEnumAddValuePlacement
}
