// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descriptormarshal

import "github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/forbiddenmethod"

// Analyzer checks for correct unmarshaling of descpb descriptors by
// disallowing calls to (descpb.Descriptor).GetTable() et al.
// (Exported from forbiddenmethod.)
var Analyzer = forbiddenmethod.DescriptorMarshalAnalyzer
