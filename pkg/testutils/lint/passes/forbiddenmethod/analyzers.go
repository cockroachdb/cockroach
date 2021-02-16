// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package forbiddenmethod defines an suite of Analyzers that
// detects correct setting of timestamps when unmarshaling table
// descriptors.
package forbiddenmethod

import "golang.org/x/tools/go/analysis"

var descriptorMarshalOptions = Options{
	PassName: "descriptormarshal",
	Doc:      `check for correct unmarshaling of descpb descriptors`,
	Package:  "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb",
	Type:     "Descriptor",
	Method:   "GetTable",
	Hint:     "see descpb.TableFromDescriptor()",
}

var grpcClientConnCloseOptions = Options{
	PassName: "grpcconnclose",
	Doc:      `prevent calls to (grpc.ClientConn).Close`,
	Package:  "google.golang.org/grpc",
	Type:     "ClientConn",
	Method:   "Close",
	Hint:     "must elide the call to Close() if the ClientConn is from an *rpc.Context",
}

// DescriptorMarshalAnalyzer checks for correct unmarshaling of descpb
// descriptors by disallowing calls to (descpb.Descriptor).GetTable().
var DescriptorMarshalAnalyzer = Analyzer(descriptorMarshalOptions)

// GRPCClientConnCloseAnalyzer checks for calls to (*grpc.ClientConn).Close.
// We mostly pull these objects from *rpc.Context, which manages their lifecycle.
// Errant calls to Close() disrupt the connection for all users.
var GRPCClientConnCloseAnalyzer = Analyzer(grpcClientConnCloseOptions)

// Analyzers are all of the Analyzers defined in this package.
var Analyzers = []*analysis.Analyzer{
	DescriptorMarshalAnalyzer,
	GRPCClientConnCloseAnalyzer,
}
