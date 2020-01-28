// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

var zeroBoolColumn = make([]bool, coldata.MaxBatchSize)

var zeroDecimalColumn = make([]apd.Decimal, coldata.MaxBatchSize)

var zeroInt16Column = make([]int16, coldata.MaxBatchSize)

var zeroInt32Column = make([]int32, coldata.MaxBatchSize)

var zeroInt64Column = make([]int64, coldata.MaxBatchSize)

var zeroFloat64Column = make([]float64, coldata.MaxBatchSize)

var zeroUint64Column = make([]uint64, coldata.MaxBatchSize)
