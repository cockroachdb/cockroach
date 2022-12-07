// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package rowinfra contains constants and types used by the row package
// that must also be accessible from other packages.
package rowinfra

import "github.com/cockroachdb/cockroach/pkg/util"

// RowLimit represents a response limit expressed in terms of number of result
// rows. RowLimits get ultimately converted to KeyLimits and are translated into
// BatchRequest.MaxSpanRequestKeys.
type RowLimit uint64

// KeyLimit represents a response limit expressed in terms of number of keys.
type KeyLimit int64

// BytesLimit represents a response limit expressed in terms of the size of the
// results. A BytesLimit ultimately translates into BatchRequest.TargetBytes.
type BytesLimit uint64

// NoRowLimit can be passed to Fetcher.StartScan to signify that the caller
// doesn't want to limit the number of result rows for each scan request.
const NoRowLimit RowLimit = 0

// NoBytesLimit can be passed to Fetcher.StartScan to signify that the caller
// doesn't want to limit the size of results for each scan request.
//
// See also defaultBatchBytesLimit.
const NoBytesLimit BytesLimit = 0

// ProductionKVBatchSize is the kv batch size to use for production (i.e.,
// non-test) clusters.
const ProductionKVBatchSize KeyLimit = 100000

// defaultBatchBytesLimit is the maximum number of bytes a scan request can
// return.
var defaultBatchBytesLimit = BytesLimit(util.ConstantWithMetamorphicTestRange(
	"default-batch-bytes-limit",
	defaultBatchBytesLimitProductionValue, /* defaultValue */
	1,                                     /* min */
	64<<10,                                /* max, 64KiB */
))

const defaultBatchBytesLimitProductionValue = 10 << 20 /* 10MiB */

// GetDefaultBatchBytesLimit returns the maximum number of bytes a scan request
// can return.
func GetDefaultBatchBytesLimit(forceProductionValue bool) BytesLimit {
	if forceProductionValue {
		return defaultBatchBytesLimitProductionValue
	}
	return defaultBatchBytesLimit
}

// SetDefaultBatchBytesLimitForTests overrides defaultBatchBytesLimit to the
// given value. This should only be used for tests when forcing the production
// via ForceProductionValues testing knob is undesirable.
func SetDefaultBatchBytesLimitForTests(v BytesLimit) {
	defaultBatchBytesLimit = v
}

// RowExecCancelCheckInterval is the default cancel check interval for the row
// execution engine.
const RowExecCancelCheckInterval = uint32(128)
