// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systembench

import "context"

// worker represents a single worker process generating load.
type worker interface {
	getLatencyHistogram() *namedHistogram
	run(ctx context.Context) error
}

var numOps uint64
var numBytes uint64
