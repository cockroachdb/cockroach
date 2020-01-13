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

var zeroBoolColumn = make([]bool, coldata.BatchSize())

var zeroDecimalColumn = make([]apd.Decimal, coldata.BatchSize())

var zeroInt16Column = make([]int16, coldata.BatchSize())

var zeroInt32Column = make([]int32, coldata.BatchSize())

var zeroInt64Column = make([]int64, coldata.BatchSize())

var zeroFloat64Column = make([]float64, coldata.BatchSize())
