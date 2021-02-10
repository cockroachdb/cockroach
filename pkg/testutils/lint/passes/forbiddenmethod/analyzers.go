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

// DescriptorMarshalAnalyzer checks for correct unmarshaling of descpb
// descriptors by disallowing calls to (descpb.Descriptor).GetTable().
var DescriptorMarshalAnalyzer = Analyzer(descriptorMarshalOptions)

// Analyzers are all of the Analyzers defined in this package.
var Analyzers = []*analysis.Analyzer{
	DescriptorMarshalAnalyzer,
}
