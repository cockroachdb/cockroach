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

// ConnectionClass is the identifier of a group of RPC client sessions that are
// allowed to share an underlying TCP connections; RPC sessions with different
// connection classes are guaranteed to use separate TCP connections.
//
// RPC sessions that share a connection class are arbitrated using the gRPC flow
// control logic, see google.golang.org/grpc/internal/transport. The lack of
// support of prioritization in the current gRPC implementation is the reason
// why we are separating different priority flows across separate TCP
// connections. Future gRPC improvements may enable further simplification
// here. See https://github.com/grpc/grpc-go/issues/1448 for progress on gRPC's
// adoption of HTTP2 priorities.
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
	switch rangeID {
	// The first few ranges, 20 at time	of writing, are set up manually by the
	// system during cluster bootstrap. The most critical of these is NodeLiveness
	// which lives in range 2 and cannot split. Other important ranges are meta1 (1)
	// and the system configuration span (6), In order to keep sql responsive we
	// additionally include the system lease table (7).
	//
	// See storage.WriteInitialClusterData, config.StaticSplits(), and
	// sqlbase.MakeMetadataSchema().GetInitialValues() to understand where these
	// initial range IDs come from.
	//
	// TODO(ajwerner): consider expanding this or using another mechanism.
	case 1, 2, 6, 7:
		return SystemClass
	}
	return DefaultClass
}
