// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// EstimateBatchSizeBytes returns an estimated amount of bytes needed to
// store a batch in memory that has column types vecTypes.
func EstimateBatchSizeBytes(vecTypes []types.T, batchLength int) int64 {
	// acc represents the number of bytes to represent a row in the batch.
	acc := int64(0)
	for _, t := range vecTypes {
		switch t {
		case types.Bool:
			acc++
		case types.Bytes:
			// We don't know without looking at the data in a batch to see how
			// much space each byte array takes up. Use some default value as a
			// heuristic right now.
			acc += 100
		case types.Int8:
			acc++
		case types.Int16:
			acc += 2
		case types.Int32:
			acc += 4
		case types.Int64:
			acc += 8
		case types.Float32:
			acc += 4
		case types.Float64:
			acc += 8
		case types.Decimal:
			// Similar to byte arrays, we can't tell how much space is used
			// to hold the arbitrary precision decimal objects.
			acc += 50
		default:
			panic(fmt.Sprintf("unhandled type %s", t))
		}
	}
	return acc * int64(batchLength)
}
