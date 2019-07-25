// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// ConnectionClass allows multiple connections to the same host to not share
// an underlying connection.
type ConnectionClass int

const (
	// DefaultClass is the default ConnectionClass and should be used for most
	// client traffic.
	DefaultClass ConnectionClass = iota
	// SystemClass is the ConnectionClass used for system traffic.
	SystemClass

	// NumConnectionClasses is the number of valid ConnectionClass values.
	NumConnectionClasses int = iota
)

// ConnectionClassForRange determines the ConnectionClass which should be used
// for traffic corresponding to this range id.
func ConnectionClassForRange(rangeID roachpb.RangeID) ConnectionClass {
	// The first 5 ranges are always system ranges.
	// TODO(ajwerner): consider expanding this or using another mechanism.
	if rangeID <= 10 {
		return SystemClass
	}
	return DefaultClass
}
